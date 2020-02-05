package com.stormpx.broker;

import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttProperty;
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

    private Long expiryTimestamp;
    private Long messageExpiryInterval=0xffffffffL;
    private int topicAlias;
    private List<Integer> subscriptionIdentifier;


    private List<MqttProperties> unalteredProperties;
    private boolean payloadFormatIndicator;
    private String responseTopic;
    private Buffer correlationData;
    private List<StringPair> userProperty;
    private String contentType;

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
        this.unalteredProperties=new ArrayList<>();
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
                .put("payload",payload.getBytes())
                .put("expiryTimestamp",expiryTimestamp);
//                .put("receiveTime",receiveTime)
//                .put("messageExpiryInterval",messageExpiryInterval);

        JsonArray array = unalteredProperties.stream()
                .map(MqttProperties::toJson)
                .collect(Collector.of(JsonArray::new, JsonArray::add, JsonArray::addAll, Collector.Characteristics.IDENTITY_FINISH));

        jsonObject.put("properties",array);

        return jsonObject;
    }


    public boolean isPayloadFormatIndicator() {
        return payloadFormatIndicator;
    }


    public Long getMessageExpiryInterval() {
        return messageExpiryInterval;
    }


    public int getTopicAlias() {
        return topicAlias;
    }

    public String getResponseTopic() {
        return responseTopic;
    }


    public Buffer getCorrelationData() {
        return correlationData;
    }


    public List<StringPair> getUserProperty() {
        return userProperty;
    }

    public String getContentType() {
        return contentType;
    }

    public List<Integer> getSubscriptionIdentifier() {
        return subscriptionIdentifier;
    }

    public List<MqttProperties> getUnalteredProperties() {
        return unalteredProperties;
    }

    public Long getExpiryTimestamp() {
        return expiryTimestamp;
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

    public MqttPublishMessage addSubscriptionIdentifier(Integer subscriptionIdentifier) {
        if (this.subscriptionIdentifier==null){
            this.subscriptionIdentifier=new ArrayList<>();
        }
        this.subscriptionIdentifier.add(subscriptionIdentifier);
        return this;
    }

    public MqttPublishMessage setMessageExpiryInterval(Long messageExpiryInterval) {
        this.messageExpiryInterval=messageExpiryInterval;
        return this;
    }

    public MqttPublishMessage setTopicAlias(int topicAlias) {
        this.topicAlias=topicAlias;
        return this;
    }


    public MqttPublishMessage setExpiryTimestamp(long expiryTimestamp){
        this.expiryTimestamp=expiryTimestamp;
        return this;
    }

    public MqttPublishMessage setPayloadFormatIndicator(boolean payloadFormatIndicator) {
        this.payloadFormatIndicator=payloadFormatIndicator;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.PAYLOAD_FORMAT_INDICATOR,payloadFormatIndicator));
        return this;
    }
    public MqttPublishMessage setResponseTopic(String responseTopic) {
        this.responseTopic = responseTopic;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.RESPONSE_TOPIC,responseTopic));
        return this;
    }

    public MqttPublishMessage setCorrelationData(Buffer correlationData) {
        this.correlationData = correlationData;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.CORRELATION_DATA,correlationData.getByteBuf()));
        return this;
    }


    public MqttPublishMessage setContentType(String contentType) {
        this.contentType=contentType;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.CONTENT_TYPE,contentType));
        return this;
    }

    public MqttPublishMessage addUserProperty(StringPair stringPair){
        this.unalteredProperties.add(new MqttProperties(MqttProperty.USER_PROPERTY,stringPair));
        if (this.userProperty==null){
            this.userProperty=new ArrayList<>();
        }
        this.userProperty.add(stringPair);
        return this;
    }




    public MqttPublishMessage setUnalteredProperties(List<MqttProperties> unalteredProperties) {
        this.unalteredProperties = unalteredProperties;
        return this;
    }
}
