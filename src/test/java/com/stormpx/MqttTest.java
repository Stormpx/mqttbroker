package com.stormpx;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.advanced.Mqtt5ClientAdvancedConfig;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperties;
import com.hivemq.client.mqtt.mqtt5.datatypes.Mqtt5UserProperty;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5RetainHandling;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscribe;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.Mqtt5Subscription;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

public class MqttTest {
    public static void main(String[] args) {
        Mqtt5BlockingClient client1= MqttClient.builder()
                .identifier("StormOne")
                .serverHost("localhost")
                .serverPort(11883)
                .useMqttVersion5()
                .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                .build().toBlocking();


        client1.connectWith().sessionExpiryInterval(300).send();


        Mqtt5Subscribe subscribe = Mqtt5Subscribe.builder().topicFilter("/test/#").qos(MqttQos.EXACTLY_ONCE).noLocal(true).retainAsPublished(false).retainHandling(Mqtt5RetainHandling.DO_NOT_SEND)
                .build();

        client1.subscribe(subscribe);

    }
}
