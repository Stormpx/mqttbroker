package com.stormpx.store;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface SessionStore {


    Future<SessionObj> get(String clientId);

    Future<Void> save(SessionObj sessionObj);

    Future<Void> del(String clientId);

    Future<Void> setExpiryTimestamp(String clientId,Long expiryTimestamp);

    Future<Long> getExpiryTimestamp(String clientId);

    Future<Void> addLink(String clientId,JsonObject link);

    Future<String> release(String clientId, int packetId);

    Future<String> receive(String clientId,int packetId);

    Future<List<JsonObject>> links(String clientId);


    Future<Void> addPacketId(String clientId,int packetId);

    Future<List<Integer>> packetId(String clientId);

    Future<Void> removePacketId(String clientId,int packetId);

    Future<Void> saveWill(String clientId, JsonObject will);

    Future<JsonObject> getWill(String clientId);

    Future<Void> delWill(String clientId);

    Future<Void> addSubscription(String clientId, JsonArray jsonArray);

    Future<JsonArray> fetchSubscription(String clientId);

    Future<Void> deleteSubscription(String clientId,List<String> topics);

}
