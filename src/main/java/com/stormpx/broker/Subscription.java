package com.stormpx.broker;

import io.netty.handler.codec.mqtt.MqttQoS;

public class Subscription {
    private String clientId;
    private String topic;
    private boolean noLocal;
    private boolean retainAsPublished;
    private MqttQoS mqttQoS;
    private int subscriptionIdentifier;
    private boolean share;

    public Subscription(String clientId, String topic, boolean noLocal, boolean retainAsPublished, MqttQoS mqttQoS, int subscriptionIdentifier, boolean share) {
        this.clientId = clientId;
        this.topic = topic;
        this.noLocal = noLocal;
        this.retainAsPublished = retainAsPublished;
        this.mqttQoS = mqttQoS;
        this.subscriptionIdentifier = subscriptionIdentifier;
        this.share = share;
    }


    public String getTopic() {
        return topic;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isNoLocal() {
        return noLocal;
    }

    public boolean isRetainAsPublished() {
        return retainAsPublished;
    }

    public MqttQoS getMqttQoS() {
        return mqttQoS;
    }

    public int getSubscriptionIdentifier() {
        return subscriptionIdentifier;
    }

    public boolean isShare() {
        return share;
    }
}
