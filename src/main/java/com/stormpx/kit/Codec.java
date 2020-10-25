package com.stormpx.kit;

import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class Codec {

    public static byte[] encode(MessageLink link){
        return Json.encodeToBuffer(link).getBytes();

    }
    public static MessageLink decodeMessageLink(byte[] bytes){
        return Json.decodeValue(Buffer.buffer(bytes),MessageLink.class);
    }

    public static byte[] encode(DispatcherMessage message){
        JsonObject json=new JsonObject();
        return json.toBuffer().getBytes();
    }

    public static DispatcherMessage decodeDispatcherMessage(byte[] bytes){
        return Json.decodeValue(Buffer.buffer(bytes),DispatcherMessage.class);
    }

    public static byte[] encode(List<MqttSubscription> subscriptionList){
        return subscriptionList.stream()
                .map(subscription -> {
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.put("topicFilter",subscription.getTopicFilter())
                            .put("qos",subscription.getQos().value())
                            .put("noLocal",subscription.isNoLocal())
                            .put("retainAsPublished",subscription.isRetainAsPublished())
                            .put("subscriptionId",subscription.getSubscriptionId())
                    ;
                    return jsonObject;
                })
                .collect(J.toJsonArray())
                .toBuffer()
                .getBytes();

    }
    public static List<MqttSubscription> decodeMqttSubscriptionList(byte[] bytes){

        return J.toJsonStream(new JsonArray(Buffer.buffer(bytes)))
                .map(json-> new MqttSubscription(json.getString("topicFilter"), MqttQoS.valueOf(json.getInteger("qos")),
                        json.getBoolean("noLocal"),json.getBoolean("retainAsPublished"),null)
                        .setSubscriptionId(json.getInteger("subscriptionId")))
                .collect(Collectors.toList());
    }

    public static byte[] encode(MqttSubscription subscription){
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("topicFilter",subscription.getTopicFilter())
                .put("qos",subscription.getQos().value())
                .put("noLocal",subscription.isNoLocal())
                .put("retainAsPublished",subscription.isRetainAsPublished())
                .put("subscriptionId",subscription.getSubscriptionId())
        ;
        return jsonObject.toBuffer().getBytes();
    }

    public static MqttSubscription decodeMqttSubscription(byte[] bytes){
        JsonObject json = Buffer.buffer(bytes).toJsonObject();
        return new MqttSubscription(json.getString("topicFilter"), MqttQoS.valueOf(json.getInteger("qos")),
                json.getBoolean("noLocal"),json.getBoolean("retainAsPublished"),null)
                .setSubscriptionId(json.getInteger("subscriptionId"));
    }


}
