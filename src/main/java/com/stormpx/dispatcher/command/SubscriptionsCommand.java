package com.stormpx.dispatcher.command;

import com.stormpx.mqtt.MqttSubscription;

import java.util.List;

public class SubscriptionsCommand {
    private String id;
    private String clientId;
    private List<MqttSubscription> mqttSubscriptions;
    private List<String> matchTopics;
    private String address;


    public String getId() {
        return id;
    }

    public SubscriptionsCommand setId(String id) {
        this.id = id;
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public SubscriptionsCommand setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public List<MqttSubscription> getMqttSubscriptions() {
        return mqttSubscriptions;
    }

    public SubscriptionsCommand setMqttSubscriptions(List<MqttSubscription> mqttSubscriptions) {
        this.mqttSubscriptions = mqttSubscriptions;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public SubscriptionsCommand setAddress(String address) {
        this.address = address;
        return this;
    }

    public List<String> getMatchTopics() {
        return matchTopics;
    }

    public SubscriptionsCommand setMatchTopics(List<String> matchTopics) {
        this.matchTopics = matchTopics;
        return this;
    }
}
