package com.stormpx.kit;

import com.stormpx.dispatcher.ClientSession;
import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Codec {

    public static byte[] encode(MessageLink link){
        return link.toJson().toBuffer().getBytes();

    }
    public static MessageLink decodeMessageLink(byte[] bytes){
        JsonObject json = Buffer.buffer(bytes).toJsonObject();
        var link=MessageLink.create(json.getString("id"),json.getString("clientId"),json.getInteger("packetId")
                ,json.getBoolean("retain"), MqttQoS.valueOf(json.getInteger("qos"))
                , Optional.ofNullable(json.getJsonArray("subscriptionId")).orElse(J.EMPTY_ARRAY).getList());
        return link;
    }

    public static byte[] encode(DispatcherMessage message){
        JsonObject json=new JsonObject();
        json.put("id",message.getId());
        json.put("topic",message.getTopic());
        json.put("qos",message.getQos().value());
        json.put("retain",message.isRetain());
        json.put("dup",message.isDup());
        json.put("properties",message.getProperties().stream().map(MqttProperties::toJson).collect(Collectors.toList()));
        json.put("messageExpiryTimestamp",message.getMessageExpiryTimestamp());
        json.put("payload",message.getPayload());

        return json.toBuffer().getBytes();
    }

    public static DispatcherMessage decodeDispatcherMessage(Buffer buffer){
        JsonObject json = buffer.toJsonObject();
        DispatcherMessage message = new DispatcherMessage();
        message.setId(json.getString("id"));
        message.setQos(MqttQoS.valueOf(json.getInteger("qos")));
        message
                .setTopic(json.getString("topic"))
                .setRetain(json.getBoolean("retain"))
                .setDup(json.getBoolean("dup"))
                .setProperties(J.toJsonStream(json.getJsonArray("properties")).map(MqttProperties::fromJson).collect(Collectors.toList()))
                .setMessageExpiryTimestamp(json.getLong("messageExpiryTimestamp"))
                .setPayload(Buffer.buffer(json.getBinary("payload")));

        return message;
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

    public static Buffer encode(ClientSession clientSession){
        JsonObject json=new JsonObject();
        json.put("clientId",clientSession.getClientId());
        json.put("expiryTimestamp",clientSession.getExpiryTimestamp());
        json.put("messageLinks",clientSession.getMessageLinks().stream().map(Codec::encode).collect(J.toJsonArray()));
        json.put("topicSubscriptions",Codec.encode(clientSession.getTopicSubscriptions()));
        json.put("will",encode(clientSession.getWill()));
        json.put("packetBit",clientSession.getPacketIdSet().toByteArray());
        return json.toBuffer();
    }

    public static ClientSession decodeClientSession(Buffer buffer){
        JsonObject json = buffer.toJsonObject();
        ClientSession clientSession=new ClientSession(json.getString("clientId"));
        clientSession.setExpiryTimestamp(json.getLong("expiryTimestamp"))
                .setTopicSubscriptions(Codec.decodeMqttSubscriptionList(json.getBinary("topicSubscriptions")))
                .setWill(Codec.decodeDispatcherMessage(Buffer.buffer(json.getBinary("will"))))
                .setPacketIdSet(BitSet.valueOf(json.getBinary("packetBit")));

        JsonArray messageLinks = json.getJsonArray("messageLinks");
        List<MessageLink> links=new ArrayList<>();
        for (int i = 0; i < messageLinks.size(); i++) {
            links.add(Codec.decodeMessageLink(messageLinks.getBinary(i)));
        }


        return clientSession;
    }
}
