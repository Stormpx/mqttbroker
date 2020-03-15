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
import io.netty.util.internal.ThreadLocalRandom;
import io.vertx.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;

public class MqttTest {

    public static void main(String[] args) {
        for (int i = 0; i < 200; i++) {
            String string = randomString();
            System.out.println(string);
            Mqtt5BlockingClient client1= MqttClient.builder()
                    .identifier(string)
                    .serverHost("localhost")
                    .serverPort(11883)
                    .useMqttVersion5()
//                    .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                    .build().toBlocking();


            client1.connectWith().sessionExpiryInterval(5999).send();
            client1.disconnect();
        }


    }

    private static String randomString(){
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb=new StringBuilder(7);
        for (int i = 0; i < 7; i++) {
            char c = (char) (random.nextInt(0, 25) + 65);
            sb.append(c);
        }
        return sb.toString();
    }
}
