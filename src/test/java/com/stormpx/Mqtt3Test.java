package com.stormpx;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5RetainHandling;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import com.stormpx.kit.UnSafeJsonObject;
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

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static com.stormpx.Constants.*;

@ExtendWith(VertxExtension.class)
public class Mqtt3Test {


    @BeforeAll
    static void beforeClass(Vertx vertx, VertxTestContext context) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        LoggerFactory.initialise();

        MqttBroker.start(vertx,new JsonObject().put("auth","echo").put("log_level","debug").put(TCP,new JsonObject().put("port",11883)).put(SAVE_DIR,"D:\\foo/")).setHandler(context.completing());


    }

    @Test
    public void subscribeTest(){
        Mqtt3BlockingClient client1= com.hivemq.client.mqtt.MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion3()
                .build().toBlocking();
        client1.connect();

        client1.subscribeWith()
                .addSubscription()
                .topicFilter("/test/qos0").qos(MqttQos.AT_MOST_ONCE)
                .applySubscription()
                .addSubscription()
                .topicFilter("/test/qos1").qos(MqttQos.AT_LEAST_ONCE)
                .applySubscription()
                .addSubscription()
                .topicFilter("/test/qos2").qos(MqttQos.EXACTLY_ONCE)
                .applySubscription()
        .send();


    }

    @Test
    public void unSubscribeTest(){
        Mqtt3BlockingClient client1= com.hivemq.client.mqtt.MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion3()
                .build().toBlocking();
        client1.connect();

        client1.unsubscribeWith()
                .addTopicFilter("/test/qos0")
                .addTopicFilter("/test/qos1")
                .addTopicFilter("/test/qos2")
                .send();

    }

    @Test
    public void publishTest(){

        Mqtt3BlockingClient client1= com.hivemq.client.mqtt.MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion3()
                .build().toBlocking();
        client1.connect();

        Mqtt3BlockingClient client2= com.hivemq.client.mqtt.MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion3()
                .build().toBlocking();
        client2.connect();

        client1.subscribeWith()
                .addSubscription()
                .topicFilter("/test/qos0").qos(MqttQos.AT_MOST_ONCE)
                .applySubscription()
                .addSubscription()
                .topicFilter("/test/qos1").qos(MqttQos.AT_LEAST_ONCE)
                .applySubscription()
                .addSubscription()
                .topicFilter("/test/qos2").qos(MqttQos.EXACTLY_ONCE)
                .applySubscription()
                .send();


        Mqtt3BlockingClient.Mqtt3Publishes publishes = client1.publishes(MqttGlobalPublishFilter.ALL);

        try {

            client2.publishWith().topic("/test/qos0").qos(MqttQos.AT_MOST_ONCE).payload(new byte[]{1}).send();
            Mqtt3Publish publish = publishes.receive();
            Assertions.assertEquals(publish.getTopic().toString(),"/test/qos0");
            Assertions.assertEquals(publish.getQos(),MqttQos.AT_MOST_ONCE);

            client2.publishWith().topic("/test/qos1").qos(MqttQos.AT_LEAST_ONCE).payload(new byte[]{1}).send();
            publish = publishes.receive();
            Assertions.assertEquals(publish.getTopic().toString(),"/test/qos1");
            Assertions.assertEquals(publish.getQos(),MqttQos.AT_LEAST_ONCE);

            client2.publishWith().topic("/test/qos2").qos(MqttQos.EXACTLY_ONCE).payload(new byte[]{1}).send();
            publish = publishes.receive();
            Assertions.assertEquals(publish.getTopic().toString(),"/test/qos2");
            Assertions.assertEquals(publish.getQos(),MqttQos.EXACTLY_ONCE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

    }

    @Test
    public void retainTest(){

        Mqtt3BlockingClient client1= com.hivemq.client.mqtt.MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion3()
                .build().toBlocking();
        client1.connect();

        Mqtt3BlockingClient client2= com.hivemq.client.mqtt.MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion3()
                .build().toBlocking();
        client2.connect();


        Mqtt3BlockingClient.Mqtt3Publishes publishes = client1.publishes(MqttGlobalPublishFilter.ALL);

        try {

            client2.publishWith().topic("/retain/qos1").qos(MqttQos.AT_LEAST_ONCE).retain(true).payload(new byte[]{1}).send();

            client1.subscribeWith()
                    .addSubscription()
                    .topicFilter("/retain/qos1").qos(MqttQos.AT_LEAST_ONCE)
                    .applySubscription()
                    .send();
            Mqtt3Publish publish = publishes.receive();
            Assertions.assertEquals(publish.getTopic().toString(),"/retain/qos1");
            Assertions.assertEquals(publish.getQos(),MqttQos.AT_LEAST_ONCE);
            Assertions.assertEquals(publish.isRetain(),true);


            client2.publishWith().topic("/retain/qos2").qos(MqttQos.EXACTLY_ONCE).retain(true).payload(new byte[]{1}).send();
            client1.subscribeWith()
                    .addSubscription()
                    .topicFilter("/retain/qos2").qos(MqttQos.EXACTLY_ONCE)
                    .applySubscription()
                    .send();

            publish = publishes.receive();
            Assertions.assertEquals(publish.getTopic().toString(),"/retain/qos2");
            Assertions.assertEquals(publish.getQos(),MqttQos.EXACTLY_ONCE);
            Assertions.assertEquals(publish.isRetain(),true);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }



}
