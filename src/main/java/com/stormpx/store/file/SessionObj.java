package com.stormpx.store.file;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class SessionObj {
    private String clientId;
    private Long expiryTimestamp;
    private Map<Integer, JsonObject> messageLinkMap;
    private JsonArray topicSubscriptions;
    //pending message key id value messageLink without packetId
    private Map<String, JsonObject> pendingMessage;
    private Set<Integer> packetIdSet;
    private JsonObject will;

    public SessionObj(String clientId) {
        this.clientId=clientId;
        this.messageLinkMap=new LinkedHashMap<>();
        this.topicSubscriptions=new JsonArray();
        this.pendingMessage =new LinkedHashMap<>();
        this.packetIdSet =new HashSet<>();
    }

    public SessionObj setExpiryTimestamp(Long expiryTimestamp) {
        this.expiryTimestamp = expiryTimestamp;
        return this;
    }

    public SessionObj setWill(JsonObject will) {
        this.will = will;
        return this;
    }

    public SessionObj addMessageLink(Integer packetId, JsonObject messageLink) {
        this.messageLinkMap.put(packetId,messageLink);
        this.pendingMessage.remove(messageLink.getString("id"));
        return this;
    }
    public SessionObj addPendingId(String id,JsonObject messageLink){
        pendingMessage.put(id,messageLink);
        return this;
    }

    public SessionObj addTopicSubscription(JsonArray topicSubscriptions){
        this.topicSubscriptions.addAll(topicSubscriptions);
        return this;
    }

    public SessionObj addPacketId(Integer id){
        this.packetIdSet.add(id);
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public Long getExpiryTimestamp() {
        return expiryTimestamp;
    }

    public Map<Integer, JsonObject> getMessageLinkMap() {
        return this.messageLinkMap;
    }


    public JsonArray getTopicSubscriptions() {
        return topicSubscriptions;
    }

    public Set<Integer> getPacketIdSet() {
        return packetIdSet;
    }

    public JsonObject getWill() {
        return will;
    }

    public Map<String, JsonObject> getPendingMessage() {
        return pendingMessage;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SessionObj that = (SessionObj) o;
        return Objects.equals(clientId, that.clientId) && Objects.equals(expiryTimestamp, that.expiryTimestamp) && Objects.equals(messageLinkMap, that.messageLinkMap) && Objects.equals(topicSubscriptions, that.topicSubscriptions) && Objects.equals(pendingMessage, that.pendingMessage) && Objects.equals(packetIdSet, that.packetIdSet) && Objects.equals(will, that.will);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clientId, expiryTimestamp, messageLinkMap, topicSubscriptions, pendingMessage, packetIdSet, will);
    }

    @Override
    public String toString() {
        return "SessionObj{" + "clientId='" + clientId + '\'' + ", expiryTimestamp=" + expiryTimestamp + ", messageLinkMap=" + messageLinkMap + ", topicSubscriptions=" + topicSubscriptions + ", pendingMessage=" + pendingMessage + ", packetIdSet=" + packetIdSet + ", will=" + will + '}';
    }
}
