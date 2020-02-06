package com.stormpx.message;

import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttSubscription;

import java.util.List;

public class MqttSubscribeMessage {
    private List<MqttSubscription> mqttSubscriptions;
    private int packetIdentifier;
    private int subscriptionIdentifier;
    private List<StringPair> userProperty;

    public MqttSubscribeMessage(List<MqttSubscription> mqttSubscriptions, int packetIdentifier, int subscriptionIdentifier, List<StringPair> userProperty) {
        this.mqttSubscriptions = mqttSubscriptions;
        this.packetIdentifier = packetIdentifier;
        this.subscriptionIdentifier = subscriptionIdentifier;
        this.userProperty = userProperty;
    }

    public List<MqttSubscription> getMqttSubscriptions() {
        return mqttSubscriptions;
    }

    public int getSubscriptionIdentifier() {
        return subscriptionIdentifier;
    }

    public List<StringPair> getUserProperty() {
        return userProperty;
    }

    public int getPacketIdentifier() {
        return packetIdentifier;
    }
}
