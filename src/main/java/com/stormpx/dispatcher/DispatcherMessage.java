package com.stormpx.dispatcher;

import com.stormpx.message.MqttPublishMessage;
import com.stormpx.message.MqttWill;
import com.stormpx.message.UnalteredProperties;
import com.stormpx.mqtt.MqttProperties;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class DispatcherMessage {
    //broker id
    private String id;
    private String topic;
    private MqttQoS qos;
    private boolean retain;
    private boolean dup;
    private Buffer payload;
    private List<MqttProperties> properties;
    private Long messageExpiryTimestamp;


    public DispatcherMessage fromPublishMessage(MqttPublishMessage publishMessage){

        this.topic=publishMessage.getTopic();
        this.qos=publishMessage.getQos();
        this.retain=publishMessage.isRetain();
        this.payload=publishMessage.getPayload();
        UnalteredProperties unalteredProperties = publishMessage.getUnalteredProperties();
        if (unalteredProperties!=null){
            this.properties=unalteredProperties.getUnalteredProperties();
        }
        if (qos.value()!=0){
            this.id = UUID.randomUUID().toString().replaceAll("-", "");
        }

        return this;
    }


    public DispatcherMessage fromWill(MqttWill will){

        this.topic=will.getWillTopic();
        this.qos=will.getQos();
        this.retain=will.isRetain();
        this.payload=will.getWillPayload();
        this.properties=will.getWillProperties();
        if (qos.value()!=0){
            this.id = UUID.randomUUID().toString().replaceAll("-", "");
        }
        return this;
    }


    public DispatcherMessage copy(){
        DispatcherMessage newMessage = new DispatcherMessage();
        newMessage.id=id;
        newMessage.topic=topic;
        newMessage.qos=qos;
        newMessage.retain=retain;
        newMessage.dup=dup;
        newMessage.properties=properties;
        newMessage.messageExpiryTimestamp=messageExpiryTimestamp;
        newMessage.payload=payload.copy();

        return newMessage;
    }


    /**
     * check message already expiry
     * @return
     */
    public boolean isExpiry(){
        if (messageExpiryTimestamp==null)
            return false;
        return messageExpiryTimestamp-Instant.now().getEpochSecond()<=0;
    }


    /**
     * get remain time second
     * @return
     */
    public Long getRemainTime(){
        if (messageExpiryTimestamp==null)
            return null;
        return messageExpiryTimestamp-Instant.now().getEpochSecond();
    }


    public DispatcherMessage setQos(MqttQoS qos) {
        this.qos = qos;
        return this;
    }

    public DispatcherMessage setDup(boolean dup) {
        this.dup = dup;
        return this;
    }

    public DispatcherMessage setRetain(boolean retain) {
        this.retain = retain;
        return this;
    }

    public DispatcherMessage setMessageExpiryTimestamp(Long messageExpiryTimestamp) {
        this.messageExpiryTimestamp = messageExpiryTimestamp;
        return this;
    }

    public Long getMessageExpiryTimestamp() {
        return messageExpiryTimestamp;
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

    public Buffer getPayload() {
        return payload;
    }


    public List<MqttProperties> getProperties() {
        return properties;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
        return "DispatcherMessage{" + "id='" + id + '\'' + ", topic='" + topic + '\'' + ", qos=" + qos + ", retain=" + retain + ", dup=" + dup + ", payload=" + payload + ", properties=" + properties + ", messageExpiryTimestamp=" + messageExpiryTimestamp + '}';
    }
}
