package com.stormpx;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import org.junit.jupiter.api.Assertions;

import java.nio.charset.StandardCharsets;

public class SubTest {
    public static void main(String[] args) {
        Mqtt5BlockingClient client1= MqttClient.builder()
                .identifier("main1")
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                //                    .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();


        client1.connectWith().sessionExpiryInterval(5999).send();
        client1.subscribeWith().topicFilter("/#").retainAsPublished(true).qos(MqttQos.EXACTLY_ONCE).send();
        try (final Mqtt5BlockingClient.Mqtt5Publishes publishes = client1.publishes(MqttGlobalPublishFilter.ALL)) {
            while (true){
                Mqtt5Publish receive = publishes.receive();
//                System.out.println(receive.getTopic());
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {

        }
        client1.disconnect();
    }
}
