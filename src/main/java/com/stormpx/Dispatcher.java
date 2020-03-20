package com.stormpx;

import com.stormpx.kit.J;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.mqtt.MqttSubscription;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class Dispatcher {
    private final static String CLIENT_SESSION_PRESENT="_client_session_present_";
    private final static String SESSION_TAKEN_OVER="_session_taken_over_";
    private final static String CLIENT_ACCEPT="_client_accept_";
    private final static String MESSAGE_DISPATCHER="_message_dispatcher_";
    private final static String TOPIC_SUBSCRIBE="_topic_subscribe_";
    private final static String TOPIC_UNSUBSCRIBE="_topic_unSubscribe_";
    private final static String RETAIN_MATCH="_retain_match_";
    private final static String TOPIC_RELOAD="_topic_reload_";
    private final static String MESSAGE_RESEND="_message_resend_";

    private Vertx vertx;

    public Dispatcher(Vertx vertx) {
        this.vertx = vertx;
    }

    public static Consumer Consumer(Vertx vertx){
        return new Consumer(vertx);
    }

    public Future<Boolean> sessionPresent(String clientId){
        Promise<Boolean> promise=Promise.promise();
        vertx.eventBus().<Boolean>request(CLIENT_SESSION_PRESENT,clientId,ar->{
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
        vertx.eventBus().publish(SESSION_TAKEN_OVER,new JsonObject().put("id",id).put("clientId",clientId).put("sessionEnd",sessionEnd));
    }

    public void sessionAccept(String clientId,boolean cleanSession){
        vertx.eventBus().send(CLIENT_ACCEPT,new JsonObject().put("clientId",clientId).put("cleanSession",cleanSession));
    }

    public void dispatcherMessage(JsonObject message){
        vertx.eventBus().send(MESSAGE_DISPATCHER, UnSafeJsonObject.wrapper(message));
    }


    public Future<Void> subscribeTopic(String clientId, List<MqttSubscription> mqttSubscriptions, Integer identifier){
        Promise<Void> promise=Promise.promise();
        JsonArray jsonArray = mqttSubscriptions.stream().map(MqttSubscription::toJson).peek(json->json.put("identifier",identifier)).collect(J.toJsonArray());
        vertx.eventBus().send(TOPIC_SUBSCRIBE,new JsonObject().put("clientId",clientId).put("subscriptions",jsonArray), ar->{
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
        vertx.eventBus().send(TOPIC_UNSUBSCRIBE,new JsonObject().put("clientId",clientId).put("topics",new JsonArray(topics)),ar->{
            if (ar.succeeded()){
                promise.complete();
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    public void matchRetainMessage(String address,List<String> topicFilters){
        vertx.eventBus().send(RETAIN_MATCH,new JsonObject().put("address",address).put("topicFilters",new JsonArray(topicFilters)));

    }

    public Future<JsonArray> reSubscribe(String clientId){
        Promise<JsonArray> promise=Promise.promise();
        vertx.eventBus().<JsonArray>request(TOPIC_RELOAD,clientId,ar->{
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
        vertx.eventBus().send(MESSAGE_RESEND,new JsonObject().put("address",responseAddress).put("clientId",clientId));
    }


    public static class Consumer{
        private Vertx vertx;

        public Consumer(Vertx vertx) {
            this.vertx = vertx;
        }

        public Consumer sessionPresentHandler(Handler<Message<String>> handler){
            if (handler!=null) {
                vertx.eventBus().<String>localConsumer(CLIENT_SESSION_PRESENT)
                    .handler(handler);
            }
            return this;
        }

        public Consumer takenOverSessionHandler(Handler<Message<JsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<JsonObject>localConsumer(SESSION_TAKEN_OVER)
                        .handler(handler);
            }
            return this;
        }

        public Consumer sessionAcceptHandler(Handler<Message<JsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<JsonObject>localConsumer(CLIENT_ACCEPT)
                        .handler(handler);
            }
            return this;
        }
        public Consumer dispatcherMessageHandler(Handler<Message<UnSafeJsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<UnSafeJsonObject>localConsumer(MESSAGE_DISPATCHER)
                        .handler(handler);
            }
            return this;
        }

        public Consumer subscribeTopicHandler(Handler<Message<JsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<JsonObject>localConsumer(TOPIC_SUBSCRIBE)
                        .handler(handler);
            }
            return this;
        }
        public Consumer unSubscribeTopicHandler(Handler<Message<JsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<JsonObject>localConsumer(TOPIC_UNSUBSCRIBE)
                        .handler(handler);
            }
            return this;
        }
        public Consumer matchRetainMessageHandler(Handler<Message<JsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<JsonObject>localConsumer(RETAIN_MATCH)
                        .handler(handler);
            }
            return this;
        }
        public Consumer reSubscribeHandler(Handler<Message<String>> handler){
            if (handler!=null) {
                vertx.eventBus().<String>localConsumer(TOPIC_RELOAD)
                        .handler(handler);
            }
            return this;
        }
        public Consumer resendMessageHandler(Handler<Message<JsonObject>> handler){
            if (handler!=null) {
                vertx.eventBus().<JsonObject>localConsumer(MESSAGE_RESEND)
                        .handler(handler);
            }
            return this;
        }
    }

}
