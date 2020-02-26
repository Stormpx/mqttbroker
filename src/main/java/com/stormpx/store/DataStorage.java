package com.stormpx.store;

import com.stormpx.store.file.MqttStoreVerticle;
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
import java.util.stream.Collectors;

public class DataStorage  {
    private final static String SOTRE_ADDRESS="_mqtt_store_";

    private Vertx vertx;

    public DataStorage(Vertx vertx) {
        this.vertx = vertx;
    }

    public Future<Void> clearSession(String clientId) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions CLEAR_SESSION = new DeliveryOptions().addHeader("action", "clearSession");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),CLEAR_SESSION,ar->{
            if (ar.succeeded()){
                promise.complete();
            }else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Void> setExpiryTimestamp(String clientId, long expiryTimestamp) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "setExpiryTimestamp");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("expiryTimestamp",expiryTimestamp),deliveryOptions,ar->{
            if (ar.succeeded()){
                promise.complete();
            }else {
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Long> getExpiryTimestamp(String clientId) {
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "setExpiryTimestamp");

        Promise<Long> promise=Promise.promise();
        vertx.eventBus().<Long>request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                promise.complete(ar.result().body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }


    public Future<Void> link(MessageLink messageLink) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "link");
        vertx.eventBus().request(SOTRE_ADDRESS,messageLink.toJson(),deliveryOptions,ar->{
            if (ar.succeeded()) {
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Void> release(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "release");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions,ar->{
            if (ar.succeeded()) {
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Void> receive(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "receive");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions,ar->{
            if (ar.succeeded()) {
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }


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



    public Future<Void> addPacketId(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "addPacketId");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions,ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }


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

    public Future<Void> removePacketId(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "removePacketId");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("packetId",packetId),deliveryOptions,ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Void> storeWillMessage(String clientId, JsonObject will) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "storeWillMessage");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId).put("will",will),deliveryOptions,ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

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

    public Future<Void> dropWillMessage(String clientId) {
        Promise<Void> promise=Promise.promise();
        DeliveryOptions deliveryOptions = new DeliveryOptions().addHeader("action", "dropWillMessage");
        vertx.eventBus().request(SOTRE_ADDRESS,new JsonObject().put("clientId",clientId),deliveryOptions,ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }


}
