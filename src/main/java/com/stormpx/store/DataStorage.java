package com.stormpx.store;

import com.stormpx.cluster.ClusterState;
import com.stormpx.cluster.LogEntry;
import com.stormpx.mqtt.MqttSubscription;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface DataStorage {

    Future<Void> init(JsonObject config);


    Future<Void> close();


    void saveState(JsonObject state);


    void saveLog(LogEntry logEntry);

    void delLog(int start,int end);

    /**
     * delete all unReleaseMessage receivedPacketId subscription
     * @param clientId
     */
    void clearSession(String clientId);

    /**
     * return id
     * @param message
     * @return
     */
    Future<String> storeMessage(JsonObject message);

    /**
     * store expiryTimestamp second
     * @param sessionState
     */
    void storeSessionState(SessionState sessionState);

    /**
     * fetch timestamp
     * @return
     */
    Future<SessionState> fetchSessionState(String clientId);

    /**
     * unack uncomp unsend message
     * @param clientId
     * @return
     */
    Future<JsonArray> fetchUnReleaseMessage(String clientId);

    /**
     *
     * @param clientId
     * @param id
     */
    void addPendingId(String clientId,String id);

    /**
     * send on publish message
     * @param messageLink
     */
    void link(MessageLink messageLink);

    /**
     * send on publish message
     * release packetId
     * @param clientId
     * @param packetId
     */
    void release(String clientId, int packetId);

    /**
     * send on publish message
     * on qos2 receive
     * @param clientId
     * @param packetId
     */
    void receive(String clientId,int packetId);

    void storeSubscription(String clientId, List<MqttSubscription> mqttSubscriptions, Integer identifier);

    Future<JsonArray> fetchSubscription(String clientId);

    void deleteSubscription(String clientId,List<String> topics);

    void addPacketId(String clientId, int packetId);

    Future<List<Integer>> unacknowledgedPacketId(String clientId);

    void removePacketId(String clientId, int packetId);

    void storeWillMessage(String clientId, JsonObject will);

    Future<JsonObject> fetchWillMessage(String clientId);

    void dropWillMessage(String clientId);

    Future<JsonArray> filterMatchMessage(List<String> topicFilters);

    @Deprecated
    default void addTimeoutWill(TimeoutWill timeoutWill){}

    @Deprecated
    default void deleteTimeoutWill(String clientId){}
    @Deprecated
    default Future<TimeoutWill> fetchFirstTimeoutWill(){
        return null;
    }


}
