package com.stormpx;

import com.stormpx.kit.J;
import com.stormpx.mqtt.MqttSubscription;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

public class Dispatcher {
    private Vertx vertx;

    public Dispatcher(Vertx vertx) {
        this.vertx = vertx;
    }

    public void sessionAccept(String clientId,boolean cleanSession){
        vertx.eventBus().send("_client_accept_",new JsonObject().put("clientId",clientId).put("cleanSession",cleanSession));
    }

    public void dispatcherMessage(JsonObject message){
        vertx.eventBus().send("_message_dispatcher_",message);
    }


    public void subscribeTopic(String clientId, List<MqttSubscription> mqttSubscriptions, Integer identifier){
        JsonArray jsonArray = mqttSubscriptions.stream().map(MqttSubscription::toJson).peek(json->json.put("identifier",identifier)).collect(J.toJsonArray());
        vertx.eventBus().send("_topic_subscribe_",new JsonObject().put("clientId",clientId).put("subscriptions",jsonArray));
    }

    public void unSubscribeTopic(String clientId, List<String> topics){
        vertx.eventBus().send("_topic_subscribe_",new JsonObject().put("clientId",clientId).put("topics",new JsonArray(topics)));
    }

    public void matchRetainMessage(String address,List<String> topicFilters){
        vertx.eventBus().send("_retain_match_",new JsonObject().put("address",address).put("topicFilters",new JsonArray(topicFilters)));

    }

    public void resendMessage(String responseAddress,String clientId){
        vertx.eventBus().send("_message_resend_",new JsonObject().put("address",responseAddress).put("clientId",clientId));
    }


}
