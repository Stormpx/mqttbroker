package com.stormpx.store.file;

import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.DataStorage;
import com.stormpx.store.MessageLink;
import com.stormpx.store.SessionState;
import com.stormpx.store.TimeoutWill;
import com.stormpx.kit.J;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

public class FileDataStorage implements DataStorage {
    private final static String SOTRE_ADDRESS="_mqtt_store_";
    private static AtomicBoolean atomicBoolean=new AtomicBoolean(false);

    private final static  DeliveryOptions CLEAR_SESSION = new DeliveryOptions().addHeader("action", "clearSession");
    private final static DeliveryOptions STORE_MESSAGE = new DeliveryOptions().addHeader("action", "storeMessage");



    private Vertx vertx;

    public FileDataStorage(Vertx vertx) {
        this.vertx = vertx;
    }


    @Override
    public Future<Void> init(JsonObject config) {
        if (!atomicBoolean.compareAndSet(false,true)){
            return Future.succeededFuture();
        }
        Promise<Void> promise=Promise.promise();
        vertx.deployVerticle(MqttStoreVerticle.class, new DeploymentOptions().setConfig(config), ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @Override
    public Future<Void> close() {
        atomicBoolean.compareAndSet(true,false);
        return Future.succeededFuture();
    }

    @Override
    public void clearSession(String clientId) {
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),CLEAR_SESSION);
    }

    @Override
    public Future<String> storeMessage(JsonObject message) {
        String id = UUID.randomUUID().toString();
        vertx.eventBus().publish(SOTRE_ADDRESS,message.put("id",id),STORE_MESSAGE);
        return Future.succeededFuture(id);
    }

    @Override
    public void storeSessionState(SessionState sessionState) {
        if (sessionState==null)return;
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "storeSessionState");
        vertx.eventBus().publish(SOTRE_ADDRESS,sessionState.toJson(),deliveryOptions);
    }

    @Override
    public Future<SessionState> fetchSessionState(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "fetchSessionState");
        Promise<SessionState> promise=Promise.promise();
        vertx.eventBus().<JsonObject>request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                JsonObject body = ar.result().body();

                promise.complete(body==null?null:SessionState.fromJson(body));
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @Override
    public Future<JsonArray> fetchUnReleaseMessage(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "fetchUnReleaseMessage");
        Promise<JsonArray> promise=Promise.promise();
        vertx.eventBus().<JsonArray>request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                JsonArray body = ar.result().body();
                promise.complete(body);
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @Override
    public void addPendingId(String clientId, String id) {
        if (clientId==null||id==null)
            return;
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "addPendingId");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("id",id),deliveryOptions);
    }

    @Override
    public void link(MessageLink messageLink) {
        if (messageLink==null)return;
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "link");
        vertx.eventBus().publish(SOTRE_ADDRESS,messageLink.toJson(),deliveryOptions);
    }

    @Override
    public void release(String clientId, int packetId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "release");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions);
    }

    @Override
    public void receive(String clientId, int packetId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "receive");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions);
    }

    @Override
    public void storeSubscription(String clientId, List<MqttSubscription> mqttSubscriptions, Integer identifier) {
        JsonArray jsonArray = mqttSubscriptions.stream().map(MqttSubscription::toJson).peek(json->json.put("identifier",identifier)).collect(J.toJsonArray());
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "storeSubscription");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("subscriptions",jsonArray),deliveryOptions);
    }

    @Override
    public Future<JsonArray> fetchSubscription(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "fetchSubscription");
        Promise<JsonArray> promise=Promise.promise();
        vertx.eventBus().<JsonArray>request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                Message<JsonArray> result = ar.result();
                promise.complete(result.body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @Override
    public void deleteSubscription(String clientId, List<String> topics) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "deleteSubscription");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("topics",topics),deliveryOptions);
    }


    @Override
    public void addPacketId(String clientId, int packetId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "addPacketId");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions);
    }


    @Override
    public Future<List<Integer>> unacknowledgedPacketId(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "unacknowledgedPacketId");
        Promise<List<Integer>> promise=Promise.promise();
        vertx.eventBus().<JsonArray>request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                Message<JsonArray> result = ar.result();
                JsonArray body = result.body();
                if (body==null){
                    promise.complete(Collections.emptyList());
                }else {
                    promise.complete(body.stream().filter(o->o instanceof Integer).map(o->(Integer)o).collect(Collectors.toList()));
                }
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @Override
    public void removePacketId(String clientId, int packetId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "removePacketId");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions);
    }

    @Override
    public void storeWillMessage(String clientId, JsonObject will) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "storeWillMessage");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("will",will),deliveryOptions);
    }

    @Override
    public Future<JsonObject> fetchWillMessage(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "fetchWillMessage");
        Promise<JsonObject> promise=Promise.promise();
        vertx.eventBus().<JsonObject>request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                Message<JsonObject> message = ar.result();
                promise.complete(message.body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    @Override
    public void dropWillMessage(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "dropWillMessage");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions);
    }

    @Override
    public Future<JsonArray> filterMatchMessage(List<String> topicFilters) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "filterMatchMessage");
        Promise<JsonArray> promise=Promise.promise();
        vertx.eventBus().<JsonArray>request(SOTRE_ADDRESS,new JsonObject().put("topicFilters",topicFilters),deliveryOptions,ar->{
            if (ar.succeeded()){
                Message<JsonArray> result = ar.result();
                promise.complete(result.body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

   /* @Override
    public void addTimeoutWill(TimeoutWill timeoutWill) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "addTimeoutWill");
        vertx.eventBus().publish(SOTRE_ADDRESS,timeoutWill.toJson(),deliveryOptions);
    }

    @Override
    public void deleteTimeoutWill(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "deleteTimeoutWill");
        vertx.eventBus().publish(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions);
    }

    @Override
    public Future<TimeoutWill> fetchFirstTimeoutWill() {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "fetchFirstTimeoutWill");
        Promise<TimeoutWill> promise=Promise.promise();
        vertx.eventBus().<JsonObject>request(SOTRE_ADDRESS,null,deliveryOptions,ar->{
            if (ar.succeeded()){
                Message<JsonObject> message = ar.result();
                if (message.body()==null) {
                    promise.complete();
                    return;
                }
                promise.complete(message.body().mapTo(TimeoutWill.class));
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }*/
}
