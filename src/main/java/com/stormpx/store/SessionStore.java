package com.stormpx.store;

import com.stormpx.dispatcher.ClientSession;
import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.mqtt.MqttSubscription;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface SessionStore {

    Future<ClientSession> get(String clientId);


    Future<Void> save(ClientSession clientSession);

    Future<Void> del(String clientId);

    Future<Void> setExpiryTimestamp(String clientId,Long expiryTimestamp);

    Future<Long> getExpiryTimestamp(String clientId);

    Future<Void> addLink(String clientId,MessageLink link);

    Future<Void> addOfflineLink(String clientId,MessageLink link);

    Future<List<MessageLink>> links(String clientId);

    Future<String> release(String clientId, int packetId);

    Future<String> receive(String clientId,int packetId);


    Future<Void> addPacketId(String clientId,int packetId);

    Future<List<Integer>> packetId(String clientId);

    Future<Void> removePacketId(String clientId,int packetId);

    Future<Void> saveWill(String clientId, DispatcherMessage will);

    Future<DispatcherMessage> getWill(String clientId);

    Future<Void> delWill(String clientId);

    Future<Void> addSubscription(String clientId,List<MqttSubscription> subscription);

    Future<List<MqttSubscription>> getSubscription(String clientId);

    Future<Void> deleteSubscription(String clientId,List<String> topics);

}
