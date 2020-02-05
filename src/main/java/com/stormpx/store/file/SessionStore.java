package com.stormpx.store.file;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class SessionStore {
    private Map<String,SessionObj> sessionObjMap;

    public SessionStore() {
        this.sessionObjMap = new ConcurrentHashMap<>();
    }



    public SessionObj getSession(String clientId){
        return sessionObjMap.computeIfAbsent(clientId, SessionObj::new);
    }

    public SessionStore putSession(String clientId,SessionObj sessionObj){
        sessionObjMap.put(clientId,sessionObj);
        return this;
    }


    public SessionStore deleteSession(String clientId){
        sessionObjMap.remove(clientId);
        return this;
    }

    public SessionStore setExpiryTimestamp(String clientId,Long expiryTimestamp) {
        SessionObj session = getSession(clientId);
        session.setExpiryTimestamp(expiryTimestamp);
        return this;
    }

    public SessionStore setWill(String clientId,JsonObject will) {
        SessionObj session = getSession(clientId);
        session.setWill(will);
        return this;
    }

    public SessionStore addMessageLink(String clientId, JsonObject messageLink) {
        SessionObj session = getSession(clientId);
        Integer packetId = messageLink.getInteger("packetId");
        if (packetId !=null) {
            session.addMessageLink(packetId, messageLink);
        }else{
            session.addPendingId(messageLink.getString("id"),messageLink);
        }
        return this;
    }


    public String release(String clientId,Integer packetId){
        SessionObj session = getSession(clientId);
        Map<Integer, JsonObject> messageLinkMap = session.getMessageLinkMap();
        if (messageLinkMap!=null){
            JsonObject link = messageLinkMap.remove(packetId);
            return link.getString("id");
        }
        return null;
    }

    public String receive(String clientId,Integer packetId){
        SessionObj session = getSession(clientId);
        Map<Integer, JsonObject> messageLinkMap = session.getMessageLinkMap();
        if (messageLinkMap!=null){
            JsonObject jsonObject = messageLinkMap.get(packetId);
            if (jsonObject!=null) {
                synchronized(clientId+"link"+packetId) {
                    Object id = jsonObject.remove("id");
                    jsonObject.put("received", true);
                    return id.toString();
                }
            }
        }
        return null;
    }

    public Stream<JsonObject> getMessageLink(String clientId) {
        SessionObj session = getSession(clientId);
        Map<Integer, JsonObject> messageLinkMap = session.getMessageLinkMap();
        return messageLinkMap
                .keySet()
                .stream()
                .map(messageLinkMap::get)
                .filter(Objects::nonNull);
    }

    public Stream<JsonObject> getPendingId(String clientId){
        SessionObj session = getSession(clientId);
        Map<String, JsonObject> pendingMessage = session.getPendingMessage();
        return pendingMessage.keySet().stream().map(pendingMessage::get).filter(Objects::nonNull);
    }

    public void deleteSubscription(String clientId,JsonArray topics) {
        JsonArray topicSubscriptions = getTopicSubscriptions(clientId);
        if (topicSubscriptions==null)
            return;
        Iterator<Object> iterator = topicSubscriptions.iterator();
       f:while (iterator.hasNext()){
            JsonObject json= (JsonObject) iterator.next();
            for (Object topic : topics) {
                if (topic.equals(json.getString("topicFilter"))) {
                    iterator.remove();
                    continue f;
                }
            }
        }
    }



    public SessionStore addTopicSubscription(String clientId,JsonArray topicSubscriptions){
        SessionObj session = getSession(clientId);
        session.addTopicSubscription(topicSubscriptions);
        return this;
    }

    public SessionStore addPacketId(String clientId,Integer packetId){
        SessionObj session = getSession(clientId);
        session.addPacketId(packetId);
        return this;
    }

    public boolean containsPacketId(String clientId,Integer packetId) {
        SessionObj session = getSession(clientId);
        Set<Integer> idSet = session.getPacketIdSet();
        if (idSet==null)
            return false;
        return idSet.contains(packetId);
    }
    public SessionStore removePacketId(String clientId,Integer packetId) {
        SessionObj session = getSession(clientId);
        session.getPacketIdSet().remove(packetId);
        return this;
    }


    public Long getExpiryTimestamp(String clientId) {
        SessionObj session = getSession(clientId);
        return session.getExpiryTimestamp();
    }



    public JsonArray getTopicSubscriptions(String clientId) {
        SessionObj session = getSession(clientId);
        return session.getTopicSubscriptions();
    }


    public JsonObject getWill(String clientId) {
        SessionObj session = getSession(clientId);
        return session.getWill();
    }

    public Set<String> keys(){
        return sessionObjMap.keySet();
    }


}
