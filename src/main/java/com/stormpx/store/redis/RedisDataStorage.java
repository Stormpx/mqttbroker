package com.stormpx.store.redis;

import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.DataStorage;
import com.stormpx.store.MessageLink;
import com.stormpx.store.SessionState;
import com.stormpx.store.TimeoutWill;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class RedisDataStorage implements DataStorage {
    @Override
    public Future<Void> init(JsonObject config) {
        return null;
    }

    @Override
    public void clearSession(String clientId) {

    }

    @Override
    public Future<String> storeMessage(JsonObject message) {
        return null;
    }

    @Override
    public void storeSessionState(SessionState sessionState) {

    }

    @Override
    public Future<SessionState> fetchSessionState(String clientId) {
        return null;
    }

    @Override
    public Future<JsonArray> fetchUnReleaseMessage(String clientId) {
        return null;
    }

    @Override
    public void addPendingId(String clientId, String id) {

    }

    @Override
    public void link(MessageLink messageLink) {

    }

    @Override
    public void release(String clientId, int packetId) {

    }

    @Override
    public void receive(String clientId, int packetId) {

    }

    @Override
    public void storeSubscription(String clientId, List<MqttSubscription> mqttSubscriptions, Integer identifier) {

    }

    @Override
    public Future<JsonArray> fetchSubscription(String clientId) {
        return null;
    }

    @Override
    public void deleteSubscription(String clientId, List<String> topics) {

    }

    @Override
    public void addPacketId(String clientId, int packetId) {

    }

    @Override
    public Future<Boolean> containsPacketId(String clientId, int packetId) {
        return null;
    }

    @Override
    public void removePacketId(String clientId, int packetId) {

    }

    @Override
    public void storeWillMessage(String clientId, JsonObject will) {

    }

    @Override
    public Future<JsonObject> fetchWillMessage(String clientId) {
        return null;
    }

    @Override
    public void dropWillMessage(String clientId) {

    }

    @Override
    public Future<JsonArray> filterMatchMessage(List<String> topicFilters) {
        return null;
    }

    @Override
    public void addTimeoutWill(TimeoutWill timeoutWill) {

    }

    @Override
    public void deleteTimeoutWill(String clientId) {

    }

    @Override
    public Future<TimeoutWill> fetchFirstTimeoutWill() {
        return null;
    }
}
