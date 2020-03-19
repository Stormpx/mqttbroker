package com.stormpx.message;

import com.stormpx.kit.J;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttProperty;
import io.netty.handler.codec.base64.Base64;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collector;

public class MqttPublishMessage {
    private String topic;
    private MqttQoS qos;
    private boolean retain;
    private boolean dup;
    private Buffer payload;
    private int packetId;

    private Long messageExpiryInterval;
    private int topicAlias;

    private UnalteredProperties unalteredProperties;


     public MqttPublishMessage(String topic, MqttQoS qos, boolean retain, boolean dup, Buffer payload) {
        this(topic,qos,retain,dup,payload,0);
    }

     public MqttPublishMessage(String topic, MqttQoS qos, boolean retain, boolean dup, Buffer payload, int packetIdentifier) {
        this.topic = topic;
        this.qos = Objects.requireNonNull(qos);
        this.retain = retain;
        this.dup = dup;
        this.payload = payload;
        this.packetId = packetIdentifier;
    }


    public String getTopic() {
        return topic;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public boolean isRetain() {
        return retain;
    }

    public boolean isDup() {
        return dup;
    }

    public int getPacketId() {
        return packetId;
    }

    public Buffer getPayload() {
        return payload;
    }


    public JsonObject toJson(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("topic",topic)
                .put("qos",qos.value())
                .put("retain",retain)
                .put("payload",payload.getBytes());

        if (unalteredProperties!=null) {
            JsonArray array = unalteredProperties.getUnalteredProperties()
                    .stream()
                    .map(MqttProperties::toJson)
                    .collect(J.toJsonArray());

            jsonObject.put("properties", array);
        }
        return jsonObject;
    }


    public Long getMessageExpiryInterval() {
        return messageExpiryInterval;
    }


    public int getTopicAlias() {
        return topicAlias;
    }


    public MqttPublishMessage setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public MqttPublishMessage setRetain(boolean retain) {
        this.retain = retain;
        return this;
    }

    public MqttPublishMessage setPacketIdentifier(int packetIdentifier) {
        this.packetId =packetIdentifier;
        return this;
    }

    public MqttPublishMessage setTopicAlias(int topicAlias) {
        this.topicAlias=topicAlias;
        return this;
    }
    public MqttPublishMessage setMessageExpiryInterval(Long messageExpiryInterval) {
        this.messageExpiryInterval=messageExpiryInterval;
        return this;
    }

    public MqttPublishMessage setUnalteredProperties(UnalteredProperties unalteredProperties) {
        this.unalteredProperties = unalteredProperties;
        return this;
    }

    public UnalteredProperties getUnalteredProperties() {
        return unalteredProperties;
    }
}
