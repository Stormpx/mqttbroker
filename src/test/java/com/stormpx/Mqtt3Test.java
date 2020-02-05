package com.stormpx;

import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.mqtt.MqttClient;
import io.vertx.mqtt.MqttClientOptions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.HashMap;
import java.util.Map;

import static com.stormpx.Constants.*;

@ExtendWith(VertxExtension.class)
public class Mqtt3Test {

    private static MqttClient client1 = null;

    private static MqttClient client2 = null;

    @BeforeAll
    static void beforeClass(Vertx vertx, VertxTestContext context) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        LoggerFactory.initialise();
        final DeploymentOptions deploymentOptions = new DeploymentOptions();
        JsonObject mqtt = new JsonObject();
        deploymentOptions.setConfig(new JsonObject().put(MQTT,mqtt));
        vertx.deployVerticle(new MqttBrokerVerticle(), deploymentOptions,context.succeeding(v->{
            client1 = MqttClient.create(vertx, new MqttClientOptions()
                    .setClientId("client1").setAutoKeepAlive(true).setWillFlag(true).setWillQoS(1).setWillMessage("client1 die").setWillRetain(false).setWillTopic("/test/will"));
            client2 = MqttClient.create(vertx, new MqttClientOptions()
                    .setClientId("client2").setAutoKeepAlive(true).setWillFlag(true).setWillQoS(2).setWillMessage("client2 die").setWillRetain(true).setWillTopic("/test/will"));
            client1.connect(11883,"localhost",context.succeeding(ar->{
                System.out.println("client1 connect");
                try {
                    Assertions.assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED.byteValue(),ar.code().byteValue());
                    Assertions.assertFalse(ar.isSessionPresent());

                    client2.connect(11883,"localhost",context.succeeding(arr->{
                        System.out.println("client2 connect");
                        try {
                            Assertions.assertEquals(MqttConnectReturnCode.CONNECTION_ACCEPTED.byteValue(),arr.code().byteValue());
                            Assertions.assertFalse(arr.isSessionPresent());
                            context.completeNow();
                        } catch (Throwable e) {
                            e.printStackTrace();
                            context.failNow(e);
                        }
                    }));
                } catch (Throwable e) {
                    e.printStackTrace();
                    context.failNow(e);
                }
            }));
        }));


    }

    @Test
    public void subscribeTest(Vertx vertx, VertxTestContext context){
        Map<String, Integer> map=new HashMap<>();
        map.put("/test/qos0",0);
        map.put("/test/qos1",1);
        map.put("/test/qos2",2);
        client1.subscribe(map,context.succeeding(id->{
            System.out.println(id);
            client2.subscribe(map,context.completing());
        }));

    }

    @Test
    public void unSubscribeTest(Vertx vertx, VertxTestContext context){
        Checkpoint checkpoint = context.laxCheckpoint(3);
        client1.unsubscribe("/test/qos0",context.succeeding(v->checkpoint.flag()));
        client1.unsubscribe("/test/qos1",context.succeeding(v->checkpoint.flag()));
        client1.unsubscribe("/test/qos2",context.succeeding(v->checkpoint.flag()));

    }

    @Test
    public void publishTest(Vertx vertx, VertxTestContext context){
        Map<String, Integer> map=new HashMap<>();
        map.put("/test/qos0",0);
        map.put("/test/qos1",1);
        map.put("/test/qos2",2);
        Checkpoint checkpoint1 = context.laxCheckpoint(3);
        long millis = System.currentTimeMillis();
        client1.publishHandler(message->{
            System.out.println(System.currentTimeMillis()-millis);
            System.out.println(String.format("topicName: %s qos: %s dup: %s retain: %s payload: %s",
                    message.topicName(),message.qosLevel().value(),message.isDup(),message.isRetain(),message.payload().toString("utf-8")));
            switch (message.topicName()){
                case "/test/qos0":
                    Assertions.assertEquals(0,message.qosLevel().value());
                    break;
                case "/test/qos1":
                    Assertions.assertEquals(1,message.qosLevel().value());
                    break;
                case "/test/qos2":
                    Assertions.assertEquals(2,message.qosLevel().value());
                    break;
                default:
                    context.failNow(new RuntimeException("unexpected topic name"+message.topicName()));
            }
            checkpoint1.flag();
        });
        client1.subscribe(map,context.succeeding(id->{
            System.out.println(id);
            client2.subscribe(map,context.succeeding(i->{
                System.out.println("client2 subscribe");
                client2.publish("/test/qos0", Buffer.buffer("ss"), MqttQoS.AT_MOST_ONCE,false,false,context.succeeding());
                client2.publish("/test/qos1", Buffer.buffer("ss"), MqttQoS.AT_LEAST_ONCE,false,false,context.succeeding());
                client2.publish("/test/qos2", Buffer.buffer("ss"), MqttQoS.EXACTLY_ONCE,false,false,context.succeeding());
            }));
        }));

    }

    @Test
    public void retainTest(Vertx vertx,VertxTestContext context){
        Map<String, Integer> map=new HashMap<>();
        map.put("/retain/qos0",0);
        map.put("/retain/qos1",1);
        map.put("/retain/qos2",2);

        Checkpoint checkpoint = context.checkpoint(3);

        client2.publishHandler(message->{
            try {
                System.out.println("client2 "+ String.format("topicName: %s qos: %s dup: %s retain: %s payload: %s",
                        message.topicName(),message.qosLevel().value(),message.isDup(),message.isRetain(),message.payload().toString("utf-8")));

                Assertions.assertTrue(message.isRetain());
                checkpoint.flag();
            } catch (Throwable e) {
                e.printStackTrace();
                context.failNow(e);
            }
        });
        int[] a={0};
        client1.publishCompletionHandler(id->{
            a[0]++;
            System.out.println(a[0]);
            if (a[0]==3){
                client2.subscribe(map,context.succeeding());
            }
        });
        client1.subscribe(map,context.succeeding(id->{
            client1.publish("/retain/qos0",Buffer.buffer("test"),MqttQoS.AT_LEAST_ONCE,false,true,context.succeeding(i1->{
                client1.publish("/retain/qos1",Buffer.buffer("test"),MqttQoS.AT_LEAST_ONCE,false,true,context.succeeding(i2->{
                    client1.publish("/retain/qos2",Buffer.buffer("test"),MqttQoS.AT_LEAST_ONCE,false,true,context.succeeding(i3->{

                    }));
                }));
            }));

        }));
    }



}
