package com.stormpx.store.file;

import com.stormpx.kit.J;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttProperty;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.RetainHandling;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;

@ExtendWith(VertxExtension.class)
public class ObjCodecTest {


    @Test
    public void codecMessageObjTest(){
        ObjCodec objCodec = new ObjCodec();
        JsonObject json = new JsonObject();
        json.put("id","2132");
        json.put("topic","/etse/topic");
        json.put("qos",1);
        json.put("retain",true);
        json.put("expiryTimestamp",4564156L);
        json.put("payload","gwarfjsaopfjap".getBytes(StandardCharsets.UTF_8));
        JsonArray properties = new JsonArray();
        properties.add(new MqttProperties(MqttProperty.PAYLOAD_FORMAT_INDICATOR,(byte)1).toJson());
        properties.add(new MqttProperties(MqttProperty.REASON_STRING,"dwqew").toJson());
        properties.add(new MqttProperties(MqttProperty.USER_PROPERTY,new StringPair("weqw","ewqrwq")).toJson());
        properties.add(new MqttProperties(MqttProperty.SESSION_EXPIRY_INTERVAL,4561L).toJson());
        properties.add(new MqttProperties(MqttProperty.CORRELATION_DATA,Buffer.buffer("weqw").getByteBuf()).toJson());
        properties.add(new MqttProperties(MqttProperty.TOPIC_ALIAS,1234).toJson());
        json.put("properties",properties);

        MessageObj messageObj = new MessageObj(json);
        Buffer buffer = objCodec.encodeMessageObj(messageObj);
        MessageObj obj = objCodec.decodeMessageObj(buffer);

        Assertions.assertEquals(obj.getMessage(),json);

    }


    /*JsonObject jsonObject = new JsonObject();
        jsonObject.put("topic",willTopic)
            .put("qos",qos.value())
            .put("retain",retain)
                .put("payload",willPayload.getBytes())
            .put("delayInterval",willDelayInterval);

        if (willProperties!=null) {
        JsonArray array = willProperties.stream()
                .map(MqttProperties::toJson)
                .collect(J.toJsonArray());

        jsonObject.put("properties", array);
    }*/
    @Test
    public void codecSessionObjTest(){
        ObjCodec objCodec = new ObjCodec();

        JsonArray properties = new JsonArray();
        properties.add(new MqttProperties(MqttProperty.PAYLOAD_FORMAT_INDICATOR,(byte)1).toJson());
        properties.add(new MqttProperties(MqttProperty.REASON_STRING,"dwqew").toJson());
        properties.add(new MqttProperties(MqttProperty.USER_PROPERTY,new StringPair("weqw","ewqrwq")).toJson());
        properties.add(new MqttProperties(MqttProperty.SESSION_EXPIRY_INTERVAL,4561L).toJson());
        properties.add(new MqttProperties(MqttProperty.CORRELATION_DATA,Buffer.buffer("weqw").getByteBuf()).toJson());
        properties.add(new MqttProperties(MqttProperty.TOPIC_ALIAS,1234).toJson());

        SessionObj sessionObj = new SessionObj("213");
        sessionObj.setExpiryTimestamp(4567451564L);
        JsonObject will = new JsonObject();
        will.put("topic","/etest/tqwe");
        will.put("qos",0);
        will.put("retain",false);
        will.put("payload","wqewqewqewq".getBytes(StandardCharsets.UTF_8));
        will.put("delayInterval",1L);
        will.put("properties",properties);
        sessionObj.setWill(will);

        JsonObject messageLink = new JsonObject();
        messageLink.put("id","2132");
        messageLink.put("clientId","client1");
        messageLink.put("packetId",1432);
        messageLink.put("qos",2);
        messageLink.put("retain",false);
        messageLink.put("subscriptionId",new JsonArray().add(2).add(3).add(4));

        JsonObject messageLink1 = new JsonObject();
        messageLink1.put("id","2132");
        messageLink1.put("clientId","client1");
        messageLink1.put("packetId",12432);
        messageLink1.put("qos",2);
        messageLink1.put("retain",false);
        messageLink1.put("subscriptionId",new JsonArray().add(2).add(3).add(4));
        sessionObj.addMessageLink(1432,messageLink);
        sessionObj.addMessageLink(12432,messageLink1);

        JsonObject pending = new JsonObject();
        pending.put("id","2132");
        pending.put("clientId","client1");
        pending.put("qos",2);
        pending.put("retain",false);
        pending.put("subscriptionId",new JsonArray().add(2).add(3).add(4));

        sessionObj.addPendingId("2132",pending);

        sessionObj.addPacketId(1)
                .addPacketId(2);

        JsonArray topicSubscriptions = new JsonArray();

        topicSubscriptions.add(new MqttSubscription("/ets/qwe/#", MqttQoS.AT_MOST_ONCE,true,false, RetainHandling.DO_NOT_SEND).toJson());
        topicSubscriptions.add(new MqttSubscription("/weqwewq/qwe/#", MqttQoS.EXACTLY_ONCE,false,true, RetainHandling.DO_NOT_SEND).toJson());
        sessionObj.addTopicSubscription(topicSubscriptions);


        Buffer buffer = objCodec.encodeSessionObj(sessionObj);
        SessionObj obj = objCodec.decodeSessionObj(buffer);

        Assertions.assertEquals(obj,sessionObj);

    }

}
