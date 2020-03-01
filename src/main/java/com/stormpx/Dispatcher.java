package com.stormpx;

import com.stormpx.kit.J;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.mqtt.MqttSubscription;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class Dispatcher {
    private Vertx vertx;

    public Dispatcher(Vertx vertx) {
        this.vertx = vertx;
    }

    public Future<Boolean> sessionPresent(String clientId){
        Promise<Boolean> promise=Promise.promise();
        vertx.eventBus().<Boolean>request("_client_session_present",clientId,ar->{
            if (ar.succeeded()){
                Boolean r = ar.result().body();
                promise.complete(r==null?false:r);
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public void takenOverSession(String id,String clientId,boolean sessionEnd){
        vertx.eventBus().publish("_session_taken_over_",new JsonObject().put("id",id).put("clientId",clientId).put("sessionEnd",sessionEnd));
    }

    public void sessionAccept(String clientId,boolean cleanSession){
        vertx.eventBus().send("_client_accept_",new JsonObject().put("clientId",clientId).put("cleanSession",cleanSession));
    }

    public void dispatcherMessage(JsonObject message){
        vertx.eventBus().send("_message_dispatcher_", UnSafeJsonObject.wrapper(message));
    }


    public Future<Void> subscribeTopic(String clientId, List<MqttSubscription> mqttSubscriptions, Integer identifier){
        Promise<Void> promise=Promise.promise();
        JsonArray jsonArray = mqttSubscriptions.stream().map(MqttSubscription::toJson).peek(json->json.put("identifier",identifier)).collect(J.toJsonArray());
        vertx.eventBus().send("_topic_subscribe_",new JsonObject().put("clientId",clientId).put("subscriptions",jsonArray), ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public Future<Void> unSubscribeTopic(String clientId, List<String> topics){
        Promise<Void> promise=Promise.promise();
        vertx.eventBus().send("_topic_unSubscribe_",new JsonObject().put("clientId",clientId).put("topics",new JsonArray(topics)),ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public void matchRetainMessage(String address,List<String> topicFilters){
        vertx.eventBus().send("_retain_match_",new JsonObject().put("address",address).put("topicFilters",new JsonArray(topicFilters)));

    }

    public Future<JsonArray> reSubscribe(String clientId){
        Promise<JsonArray> promise=Promise.promise();
        vertx.eventBus().<JsonArray>request("_topic_reload_",clientId,ar->{
            if (ar.succeeded()){
                Message<JsonArray> result = ar.result();
                promise.complete(result.body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public void resendMessage(String responseAddress,String clientId){
        vertx.eventBus().send("_message_resend_",new JsonObject().put("address",responseAddress).put("clientId",clientId));
    }


}
