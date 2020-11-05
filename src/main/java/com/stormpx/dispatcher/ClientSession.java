package com.stormpx.dispatcher;

import com.stormpx.message.MqttWill;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;

import java.time.Instant;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

public class ClientSession {
    private String clientId;
    private Long expiryTimestamp;
    //pending message key id value messageLink without packetId
    private List<MessageLink> messageLinks;

    private List<MqttSubscription> topicSubscriptions;

    private DispatcherMessage will;
    private BitSet packetIdSet;


    public ClientSession(String clientId) {
        this.clientId = clientId;
    }


    public static boolean isExpiry(Long timestamp){
        return timestamp != null && Instant.now().getEpochSecond() >= timestamp;
    }


    public String getClientId() {
        return clientId;
    }

    public ClientSession setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public Long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    public ClientSession setExpiryTimestamp(Long expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
        return this;
    }

    public List<MessageLink> getMessageLinks() {
        return messageLinks;
    }

    public ClientSession setMessageLinks(List<MessageLink> messageLinks) {
        this.messageLinks = messageLinks;
        return this;
    }

    public List<MqttSubscription> getTopicSubscriptions() {
        return topicSubscriptions;
    }

    public ClientSession setTopicSubscriptions(List<MqttSubscription> topicSubscriptions) {
        this.topicSubscriptions = topicSubscriptions;
        return this;
    }

    public DispatcherMessage getWill() {
        return will;
    }

    public ClientSession setWill(DispatcherMessage will) {
        this.will = will;
        return this;
    }

    public BitSet getPacketIdSet() {
        return packetIdSet;
    }

    public ClientSession setPacketIdSet(BitSet packetIdSet) {
        this.packetIdSet = packetIdSet;
        return this;
    }
}
