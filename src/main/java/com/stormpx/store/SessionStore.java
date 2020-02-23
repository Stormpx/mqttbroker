package com.stormpx.store;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface SessionStore {


    Future<SessionObj> get(String clientId);

    void save(SessionObj sessionObj);

    void del(String clientId);

    void setExpiryTimestamp(String clientId,Long expiryTimestamp);

    Future<Long> getExpiryTimestamp(String clientId);

    void addLink(String clientId,JsonObject link);

    Future<String> release(String clientId, int packetId);

    Future<String> receive(String clientId,int packetId);

    Future<List<JsonObject>> links(String clientId);


    void addPacketId(String clientId,int packetId);

    Future<List<Integer>> packetId(String clientId);

    void removePacketId(String clientId,int packetId);

    void saveWill(String clientId, JsonObject will);

    Future<JsonObject> getWill(String clientId);

    void delWill(String clientId);

    void addSubscription(String clientId, JsonArray jsonArray);

    Future<JsonArray> fetchSubscription(String clientId);

    void deleteSubscription(String clientId,List<String> topics);

}
