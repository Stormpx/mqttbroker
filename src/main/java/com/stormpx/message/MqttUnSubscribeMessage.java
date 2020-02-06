package com.stormpx.message;

import com.stormpx.kit.StringPair;

import java.util.List;

public class MqttUnSubscribeMessage {
    private List<String> mqttSubscriptions;
    private int packetIdentifier;
    private List<StringPair> userProperty;


    public MqttUnSubscribeMessage(List<String> mqttSubscriptions, int packetIdentifier, List<StringPair> userProperty) {
        this.mqttSubscriptions = mqttSubscriptions;
        this.packetIdentifier = packetIdentifier;
        this.userProperty = userProperty;
    }

    public List<String> getMqttSubscriptions() {
        return mqttSubscriptions;
    }

    public int getPacketIdentifier() {
        return packetIdentifier;
    }

    public List<StringPair> getUserProperty() {
        return userProperty;
    }
}
