package com.stormpx;

import com.hivemq.client.internal.util.ByteBufferUtil;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.disconnect.Mqtt5DisconnectReasonCode;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5RetainHandling;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static com.stormpx.Constants.TCP;

@ExtendWith(VertxExtension.class)
public class Mqtt5Test {


    private static Mqtt5BlockingClient client2;


    @BeforeAll
    static void beforeClass(Vertx vertx, VertxTestContext context) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        LoggerFactory.initialise();
        DeploymentOptions mqtt = new DeploymentOptions().setConfig(new JsonObject().put("auth","echo").put(TCP,new JsonObject().put("port",11883)));
        vertx.deployVerticle(new MqttBrokerVerticle(), mqtt,context.succeeding(v->{

           /* client2 = MqttClient.builder()
                    .identifier(UUID.randomUUID().toString())
                    .serverHost("localhost")
                    .serverPort(11883)
                    .useMqttVersion5()
                    .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                    .build().toBlocking();
            client2.connect();*/

            context.completeNow();

        }));

    }

    @Test
    public void subscribeTest(){
        Mqtt5BlockingClient client1=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client1.connect();

        Mqtt5Subscribe subscribe = Mqtt5Subscribe.builder().topicFilter("/t/#").noLocal(true).retainAsPublished(false).retainHandling(Mqtt5RetainHandling.DO_NOT_SEND)
                .addSubscription(Mqtt5Subscription.builder().topicFilter("/a/#").qos(MqttQos.EXACTLY_ONCE).noLocal(false).retainAsPublished(false).build())
                .userProperties(Mqtt5UserProperties.of( Mqtt5UserProperty.of("key", "value")))
                .build();
        Mqtt5UserProperties userProperties = client1.subscribe(subscribe).getUserProperties();
        List<? extends Mqtt5UserProperty> mqtt5UserProperties = userProperties.asList();
        Mqtt5UserProperty mqtt5UserProperty = mqtt5UserProperties.get(0);
        Assertions.assertEquals(mqtt5UserProperty.getName().toString(),"key");
        Assertions.assertEquals(mqtt5UserProperty.getValue().toString(),"value");

    }

    @Test
    public void unSubscribeTest(){
        Mqtt5BlockingClient client1=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client1.connect();
        client1.unsubscribeWith().addTopicFilter("/t/#").send();
    }

    @Test
    public void publishTest(){
        Mqtt5BlockingClient client1=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client1.connect();

        Mqtt5BlockingClient client2=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client2.connect();

        client2.subscribeWith().topicFilter("/publish/+/#").retainAsPublished(true).qos(MqttQos.EXACTLY_ONCE).send();

        try (final Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL)) {

            client1.publishWith().topic("/publish/test/0").qos(MqttQos.AT_MOST_ONCE).payload("test".getBytes(StandardCharsets.UTF_8)).responseTopic("2343").contentType("json").send();
            Mqtt5Publish receive = publishes.receive();
            Assertions.assertEquals(receive.getTopic().toString(),"/publish/test/0");
            Assertions.assertEquals(receive.getQos(),MqttQos.AT_MOST_ONCE);
            client1.publishWith().topic("/publish/test/1").qos(MqttQos.AT_LEAST_ONCE).payload("dwqeqw".getBytes(StandardCharsets.UTF_8)).send();
            receive = publishes.receive();
            Assertions.assertEquals(receive.getTopic().toString(),"/publish/test/1");
            Assertions.assertEquals(receive.getQos(),MqttQos.AT_LEAST_ONCE);

            client1.publishWith().topic("/publish/test/2").qos(MqttQos.EXACTLY_ONCE).payload("111".getBytes(StandardCharsets.UTF_8)).correlationData("123213".getBytes(StandardCharsets.UTF_8)).send();
            receive = publishes.receive();
            Assertions.assertEquals(receive.getTopic().toString(),"/publish/test/2");
            Assertions.assertEquals(receive.getQos(),MqttQos.EXACTLY_ONCE);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }

    }


    @Test
    public void retainMessageTest(){
        Mqtt5BlockingClient client1=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client1.connect();

        ByteBuffer wrap = ByteBufferUtil.wrap("123".getBytes(StandardCharsets.UTF_8));
        client1.publishWith().topic("/retain/test/2").retain(true).qos(MqttQos.EXACTLY_ONCE).payload("111".getBytes(StandardCharsets.UTF_8)).correlationData(wrap).send();

        Mqtt5BlockingClient client2=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client2.connect();
        try (final Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL)) {

            client2.subscribeWith().topicFilter("/retain/#").retainAsPublished(true).qos(MqttQos.EXACTLY_ONCE).send();
            Mqtt5Publish receive = publishes.receive();
            Assertions.assertEquals(receive.getTopic().toString(),"/retain/test/2");
            Assertions.assertEquals(receive.getQos(),MqttQos.EXACTLY_ONCE);
            Assertions.assertEquals(receive.getCorrelationData().get(),wrap);
            Assertions.assertTrue(receive.isRetain());
            Assertions.assertEquals(receive.getPayload().get(), ByteBufferUtil.wrap("111".getBytes(StandardCharsets.UTF_8)));

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }

    }

    @Test
    public void willTest(){
        Mqtt5BlockingClient client1=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client1.connect();


        Mqtt5BlockingClient client2=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client2.connect();
        client2.subscribeWith().topicFilter("/will/#").qos(MqttQos.AT_LEAST_ONCE).send();
        try (final Mqtt5BlockingClient.Mqtt5Publishes publishes = client2.publishes(MqttGlobalPublishFilter.ALL)) {

            client1.disconnectWith().reasonCode(Mqtt5DisconnectReasonCode.DISCONNECT_WITH_WILL_MESSAGE).send();
            Mqtt5Publish receive = publishes.receive();

            Assertions.assertEquals(receive.getTopic().toString(),"/will/test");
            Assertions.assertEquals(receive.getQos(),MqttQos.AT_LEAST_ONCE);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }
    }

    @Test
    public void shareSubscribeTest(){
        Mqtt5BlockingClient client1=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client1.connect();


        Mqtt5BlockingClient client2=MqttClient.builder()
                .identifier(UUID.randomUUID().toString())
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();
        client2.connect();


        client1.subscribeWith().topicFilter("$share/group/#").qos(MqttQos.AT_LEAST_ONCE).send();
        client2.subscribeWith().topicFilter("$share/group/#").qos(MqttQos.AT_LEAST_ONCE).send();
        try (Mqtt5BlockingClient.Mqtt5Publishes publishes1 = client1.publishes(MqttGlobalPublishFilter.ALL);
             Mqtt5BlockingClient.Mqtt5Publishes publishes2 = client2.publishes(MqttGlobalPublishFilter.ALL)) {

            client2.publishWith().topic("/test").qos(MqttQos.EXACTLY_ONCE).send();
            AtomicInteger a= new AtomicInteger();
            publishes1.receive(200,TimeUnit.MILLISECONDS)
                .ifPresent(p->{
                    a.getAndIncrement();
                    Assertions.assertEquals(p.getTopic().toString(),"/test");
                });
            publishes2.receive(200,TimeUnit.MILLISECONDS)
                    .ifPresent(p->{
                        a.getAndIncrement();
                        Assertions.assertEquals(p.getTopic().toString(),"/test");
                    });

//            CountDownLatch countDownLatch = new CountDownLatch(2);
//            countDownLatch.await();
            Assertions.assertEquals(1,a.get());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }


    }

}
