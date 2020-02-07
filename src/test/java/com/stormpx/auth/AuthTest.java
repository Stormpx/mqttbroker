package com.stormpx.auth;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5ConnAckException;
import com.hivemq.client.mqtt.mqtt5.exceptions.Mqtt5PubAckException;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.puback.Mqtt5PubAckReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAckReasonCode;
import com.stormpx.MqttBrokerVerticle;
import com.stormpx.kit.UnSafeJsonObject;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpHeaders;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

import static com.stormpx.Constants.TCP;

@ExtendWith(VertxExtension.class)
public class AuthTest {



    @BeforeAll
    static void beforeClass(Vertx vertx, VertxTestContext context) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        LoggerFactory.initialise();
        vertx.eventBus().registerDefaultCodec(UnSafeJsonObject.class,UnSafeJsonObject.CODEC);
        context.completeNow();
    }

    @Test
    public void configAclTest(Vertx vertx, VertxTestContext context){
        JsonArray users = new JsonArray();
        users.add(
                new JsonObject()
                        .put("username","client1")
                        .put("password","password")
                        .put("ip","127.0.0.1")
                        .put("user_property",new JsonObject().put("key","value"))
        );

        JsonArray acl = new JsonArray();
        acl
                .add(
                        new JsonObject()
                                .put("client","client1")
                                .put("permission",new JsonArray()
                                        .add(
                                                new JsonObject()
                                                        .put("topic","/test/1")
                                                        .put("max_qos",1)
                                                        .put("action","both")
                                                        .put("user_property",new JsonObject().put("key","value"))
                                        ))
                );

        DeploymentOptions mqtt = new DeploymentOptions().setConfig(new JsonObject().put("auth","config")
                .put("users",users)
                .put("acl",acl)
                .put(TCP,new JsonObject().put("port",11883)));

        vertx.deployVerticle(new MqttBrokerVerticle(), mqtt,context.succeeding(v->{
            try {
                Mqtt5BlockingClient client1= MqttClient.builder()
                        .identifier("client1")
                        .serverHost("localhost")
                        .serverPort(11883)
                        .useMqttVersion5()
                        .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                        .build().toBlocking();
                Mqtt5ConnAck mqtt5ConnAck = client1.connectWith().simpleAuth().username("client1").password("password".getBytes(StandardCharsets.UTF_8)).applySimpleAuth().send();
                Assertions.assertEquals(mqtt5ConnAck.getReasonCode(), Mqtt5ConnAckReasonCode.SUCCESS);
                Mqtt5UserProperties userProperties =
                        mqtt5ConnAck.getUserProperties();
                Mqtt5UserProperty mqtt5UserProperty = userProperties.asList().get(0);
                Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
                Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

                Mqtt5SubAck mqtt5SubAck = client1.subscribeWith().topicFilter("/test/1").qos(MqttQos.EXACTLY_ONCE).noLocal(true).send();

                Assertions.assertEquals(mqtt5SubAck.getReasonCodes().get(0), Mqtt5SubAckReasonCode.GRANTED_QOS_1);
                mqtt5UserProperty = mqtt5SubAck.getUserProperties().asList().get(0);
                Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
                Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

                Mqtt5PublishResult.Mqtt5Qos1Result publishResult =(Mqtt5PublishResult.Mqtt5Qos1Result) client1.publishWith().topic("/test/1").qos(MqttQos.AT_LEAST_ONCE).send();

                mqtt5UserProperty =publishResult.getPubAck().getUserProperties().asList().get(0);
                Assertions.assertEquals(publishResult.getPubAck().getReasonCode(), Mqtt5PubAckReasonCode.SUCCESS);
                Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
                Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

                try {
                    client1.publishWith().topic("/test/2").qos(MqttQos.AT_LEAST_ONCE).send();
                    //                mqtt5UserProperty =publishResult.getPubAck().getUserProperties().asList().get(0);
                } catch (Mqtt5PubAckException e) {
                    Mqtt5PubAck mqtt5PubAck = e.getMqttMessage();
                    Assertions.assertEquals(mqtt5PubAck.getReasonCode(), Mqtt5PubAckReasonCode.NOT_AUTHORIZED);
                }


                try {
                    Mqtt5BlockingClient client2= MqttClient.builder()
                            .identifier("client1")
                            .serverHost("localhost")
                            .serverPort(11883)
                            .useMqttVersion5()
                            .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                            .build().toBlocking();
                    mqtt5ConnAck = client2.connectWith().simpleAuth().username("client2").password("password".getBytes(StandardCharsets.UTF_8)).applySimpleAuth().send();

                } catch (Mqtt5ConnAckException e) {

                    Assertions.assertEquals(e.getMqttMessage().getReasonCode(), Mqtt5ConnAckReasonCode.BAD_USER_NAME_OR_PASSWORD);
                }

                vertx.undeploy(v,context.completing());
            } catch (Exception e) {
                context.failNow(e);
            }
        }));
    }

    @Test
    public void httpAuthTest(Vertx vertx, VertxTestContext context){
        vertx.createHttpServer().requestHandler(request->{
            request.bodyHandler(b->{
                JsonObject jsonObject = b.toJsonObject();
                System.out.println(jsonObject.encodePrettily());
                if (request.path().contains("auth")) {
                    String username = jsonObject.getString("username");
                    if ("client1".equals(username)) {
                        request.response().setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON).end(new JsonObject().put("user_property",new JsonObject().put("key", "value")).toBuffer());
                    } else {
                        request.response().setStatusCode(403).end();
                    }
                }else{

                    String action = jsonObject.getString("action");
                    if (action.equals("pub")){
                        String topic = jsonObject.getString("topic");
                        if ("/test/1".equals(topic)){
                            request.response().setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON).end(new JsonObject().put("user_property",new JsonObject().put("key", "value")).toBuffer());
                        }else{
                            request.response().setStatusCode(403).end();
                        }
                    }else{
                        JsonArray topicFilter = jsonObject.getJsonArray("topicFilters");
                        if (topicFilter.size()!=1){
                            request.response().setStatusCode(200).end();
                        }else{
                            JsonObject j = topicFilter.getJsonObject(0);
                            String topic = j.getString("topic");
                            if ("/test/3".equals(topic)) {
                                JsonObject result = new JsonObject();
                                JsonArray topicFilters = new JsonArray();
                                topicFilters.add(new JsonObject().put("topic","/test/3").put("qos",1));
                                result.put("topicFilters",topicFilters);
                                request.response().setStatusCode(200).putHeader(HttpHeaders.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON).end(result.put("user_property",new JsonObject().put("key", "value")).toBuffer());
                            }else{
                                request.response().setStatusCode(403).end();
                            }
                        }
                    }
                }
            });
        }).listen(9999,context.succeeding(s->{

        }));


        DeploymentOptions mqtt = new DeploymentOptions().setConfig(new JsonObject().put("auth","http")
                .put("appkey","appkey")
                .put("http_authentication_url","http://localhost:9999/auth")
                .put("http_authorization_url","http://localhost:9999/aza")
                .put(TCP,new JsonObject().put("port",11883)));

        vertx.deployVerticle(new MqttBrokerVerticle(), mqtt,context.succeeding(v->{
            try {
                Mqtt5BlockingClient client1= MqttClient.builder()
                        .identifier("client1")
                        .serverHost("localhost")
                        .serverPort(11883)
                        .useMqttVersion5()
                        .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                        .build().toBlocking();
                Mqtt5ConnAck mqtt5ConnAck = client1.connectWith().simpleAuth().username("client1").password("password".getBytes(StandardCharsets.UTF_8)).applySimpleAuth().send();
                Assertions.assertEquals(mqtt5ConnAck.getReasonCode(), Mqtt5ConnAckReasonCode.SUCCESS);
                Mqtt5UserProperties userProperties =
                        mqtt5ConnAck.getUserProperties();
                Mqtt5UserProperty mqtt5UserProperty = userProperties.asList().get(0);
                Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
                Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

                client1.subscribeWith().addSubscription().topicFilter("/test/4").qos(MqttQos.EXACTLY_ONCE).noLocal(true).applySubscription()
                        .addSubscription().topicFilter("/test/5").qos(MqttQos.EXACTLY_ONCE).noLocal(true).applySubscription()
                        .send();
                Mqtt5SubAck mqtt5SubAck = client1.subscribeWith().topicFilter("/test/3").qos(MqttQos.EXACTLY_ONCE).noLocal(true).send();

                Assertions.assertEquals(mqtt5SubAck.getReasonCodes().get(0), Mqtt5SubAckReasonCode.GRANTED_QOS_1);
                mqtt5UserProperty = mqtt5SubAck.getUserProperties().asList().get(0);
                Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
                Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

                Mqtt5PublishResult.Mqtt5Qos1Result publishResult =(Mqtt5PublishResult.Mqtt5Qos1Result) client1.publishWith().topic("/test/1").qos(MqttQos.AT_LEAST_ONCE).send();

                mqtt5UserProperty =publishResult.getPubAck().getUserProperties().asList().get(0);
                Assertions.assertEquals(publishResult.getPubAck().getReasonCode(), Mqtt5PubAckReasonCode.SUCCESS);
                Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
                Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

                try {
                    client1.publishWith().topic("/test/2").qos(MqttQos.AT_LEAST_ONCE).send();
                    //                mqtt5UserProperty =publishResult.getPubAck().getUserProperties().asList().get(0);
                } catch (Mqtt5PubAckException e) {
                    Mqtt5PubAck mqtt5PubAck = e.getMqttMessage();
                    Assertions.assertEquals(mqtt5PubAck.getReasonCode(), Mqtt5PubAckReasonCode.NOT_AUTHORIZED);
                }
                vertx.undeploy(v,context.completing());
            } catch (Exception e) {
                context.failNow(e);
            }
        }));


    }

}
