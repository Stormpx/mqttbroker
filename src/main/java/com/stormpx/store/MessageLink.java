package com.stormpx.store;

import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class MessageLink {
    private String id;
    private String clientId;
    private Integer packetId;
    private boolean retain;
    private MqttQoS qos;
    private List<Integer> subscriptionId;

     MessageLink(String id, String clientId, Integer packetId, boolean retain, MqttQoS qos, List<Integer> subscriptionId) {
        this.id = id;
        this.clientId = clientId;
        this.packetId = packetId;
        this.retain = retain;
        this.qos = qos;
        this.subscriptionId = subscriptionId;
    }

    public static MessageLink create(String id, String clientId, Integer packetId, boolean retain, MqttQoS qos, List<Integer> subscriptionId){
        return new MessageLink(id, clientId, packetId, retain, qos, subscriptionId);
    }

    public boolean isOffLink(){
         return packetId==null;
    }

    public boolean isRetain() {
        return retain;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public List<Integer> getSubscriptionId() {
        return subscriptionId;
    }

    public String getId() {
        return id;
    }

    public String getClientId() {
        return clientId;
    }

    public Integer getPacketId() {
        return packetId;
    }

    public JsonObject toJson(){
        JsonObject json = new JsonObject();
        json.put("id",id);
        json.put("clientId",clientId);
        json.put("packetId",packetId);
        json.put("retain",retain);
        json.put("qos",qos.value());
        json.put("subscriptionId",subscriptionId);

        return json;

    }
}
