package com.stormpx.broker;


import com.stormpx.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;

import java.util.List;

public class MqttBrokerMessage {

    private String id;
    private String clientId;
    private Integer packetId;
    private String topic;
    private MqttQoS qos ;
    private boolean retain;
    private boolean dup ;
    private Buffer payload ;
    private List<MqttProperties> properties;
    private List<Integer> subscriptionId;
    private Long expiryTimestamp;
    private Long messageExpiryInterval;



    public String getId() {
        return id;
    }

    public MqttBrokerMessage setId(String id) {
        this.id = id;
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public MqttBrokerMessage setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public Integer getPacketId() {
        return packetId;
    }

    public MqttBrokerMessage setPacketId(Integer packetId) {
        this.packetId = packetId;
        return this;
    }

    public String getTopic() {
        return topic;
    }

    public MqttBrokerMessage setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public MqttBrokerMessage setQos(MqttQoS qos) {
        this.qos = qos;
        return this;
    }

    public boolean isRetain() {
        return retain;
    }

    public MqttBrokerMessage setRetain(boolean retain) {
        this.retain = retain;
        return this;
    }

    public boolean isDup() {
        return dup;
    }

    public MqttBrokerMessage setDup(boolean dup) {
        this.dup = dup;
        return this;
    }

    public Buffer getPayload() {
        return payload;
    }

    public MqttBrokerMessage setPayload(Buffer payload) {
        this.payload = payload;
        return this;
    }

    public List<MqttProperties> getProperties() {
        return properties;
    }

    public MqttBrokerMessage setProperties(List<MqttProperties> properties) {
        this.properties = properties;
        return this;
    }

    public List<Integer> getSubscriptionId() {
        return subscriptionId;
    }

    public MqttBrokerMessage setSubscriptionId(List<Integer> subscriptionId) {
        this.subscriptionId = subscriptionId;
        return this;
    }

    public Long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    public MqttBrokerMessage setExpiryTimestamp(Long expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
        return this;
    }

    public Long getMessageExpiryInterval() {
        return messageExpiryInterval;
    }

    public MqttBrokerMessage setMessageExpiryInterval(Long messageExpiryInterval) {
        this.messageExpiryInterval = messageExpiryInterval;
        return this;
    }

    @Override
    public String toString() {
        return "MqttBrokerMessage{" + "id='" + id + '\'' + ", clientId='" + clientId + '\'' + ", packetId=" + packetId + ", topic='" + topic + '\'' + ", qos=" + qos + ", retain=" + retain + ", dup=" + dup + ", payload=" + payload + ", properties=" + properties + ", subscriptionId=" + subscriptionId + ", expiryTimestamp=" + expiryTimestamp + ", messageExpiryInterval=" + messageExpiryInterval + '}';
    }
}
