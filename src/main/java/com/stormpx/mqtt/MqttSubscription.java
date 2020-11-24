package com.stormpx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

public class MqttSubscription {
    private String topicFilter;
    private MqttQoS qos;
    private boolean noLocal;
    private boolean retainAsPublished;
    private RetainHandling retainHandling;
    private int subscriptionId;


    public MqttSubscription(String topicFilter, MqttQoS qos, boolean noLocal, boolean retainAsPublished, RetainHandling retainHandling) {
        this.topicFilter = topicFilter;
        this.qos = qos;
        this.noLocal = noLocal;
        this.retainAsPublished = retainAsPublished;
        this.retainHandling = retainHandling;
    }



    public String getTopicFilter() {
        return topicFilter;
    }


    public boolean isNoLocal() {
        return noLocal;
    }

    public boolean isRetainAsPublished() {
        return retainAsPublished;
    }

    public MqttSubscription setQos(MqttQoS qos) {
        this.qos = qos;
        return this;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public RetainHandling getRetainHandling() {
        return retainHandling;
    }


    public JsonObject toJson(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("topicFilter",topicFilter)
                .put("qos",qos.value())
                .put("noLocal",noLocal)
                .put("retainAsPublished",retainAsPublished);

        return jsonObject;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttSubscription that = (MqttSubscription) o;
        return noLocal == that.noLocal && retainAsPublished == that.retainAsPublished && subscriptionId == that.subscriptionId && Objects.equals(topicFilter, that.topicFilter) && qos == that.qos && retainHandling == that.retainHandling;
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicFilter, qos, noLocal, retainAsPublished, retainHandling, subscriptionId);
    }

    @Override
    public String toString() {
        return "MqttSubscription{" + "topicFilter='" + topicFilter + '\'' + ", qos=" + qos + ", noLocal=" + noLocal + ", retainAsPublished=" + retainAsPublished + ", retainHandling=" + retainHandling + ", subscriptionId=" + subscriptionId + '}';
    }

    public int getSubscriptionId() {
        return subscriptionId;
    }

    public MqttSubscription setSubscriptionId(int subscriptionId) {
        this.subscriptionId = subscriptionId;
        return this;
    }
}
