package com.stormpx.codec;

import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.*;
import com.stormpx.mqtt.packet.*;
import io.netty.buffer.*;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Handler;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

@ExtendWith(VertxExtension.class)
public class CodecTest {


    @Test
    public void connectPacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();
        MqttConnectPacket packet = new MqttConnectPacket(FixedHeader.CONNECT,
                MqttVersion.MQTT_5_0, "123", false, 20, new ArrayList<>(),
                true, false, new ArrayList<>(), MqttQoS.AT_MOST_ONCE, "/will", Unpooled.copiedBuffer("test".getBytes(StandardCharsets.UTF_8)),
                "231", Unpooled.copiedBuffer("312".getBytes(StandardCharsets.UTF_8)));
        List<Object> list=new ArrayList<>();
        mqttEncoder.encode(packet,list);
        mqttDecoder.decode(((ByteBuf) list.remove(0)).copy(),list);
        packet.getWillPayload().resetReaderIndex();
        packet.getPassword().resetReaderIndex();

        System.out.println(list.get(0));
        System.out.println(packet);
        Assertions.assertEquals(list.get(0),packet);

    }

    @Test
    public void connectAckPacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();

        List<MqttProperties> properties = Arrays.asList(new MqttProperties(MqttProperty.SESSION_EXPIRY_INTERVAL, 12L),
                new MqttProperties(MqttProperty.RECEIVE_MAXIMUM, 30),
                new MqttProperties(MqttProperty.MAXIMUM_QOS, (byte) 1),
                new MqttProperties(MqttProperty.RETAIN_AVAILABLE, (byte) 1),
                new MqttProperties(MqttProperty.MAXIMUM_PACKET_SIZE, 456L),
                new MqttProperties(MqttProperty.TOPIC_ALIAS_MAXIMUM,65000),
                new MqttProperties(MqttProperty.REASON_STRING, "ewqew"),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "32142")),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "321awew42")),
                new MqttProperties(MqttProperty.WILDCARD_SUBSCRIPTION_AVAILABLE, (byte) 1),
                new MqttProperties(MqttProperty.SUBSCRIPTION_IDENTIFIER_AVAILABLE, (byte) 1),
                new MqttProperties(MqttProperty.SHARED_SUBSCRIPTION_AVAILABLE, (byte) 1));

        MqttConnAckPacket packet = new MqttConnAckPacket(FixedHeader.CONNACK, ReasonCode.SUCCESS.byteValue(), properties, true);

        List<Object> list=new ArrayList<>();

        mqttEncoder.encode(packet,list);
        Assertions.assertTrue(!list.isEmpty());
        mqttDecoder.decode(((ByteBuf) list.remove(0)).copy(),list);
        Assertions.assertTrue(!list.isEmpty());


        Assertions.assertEquals(list.get(0),packet);
    }

    @Test
    public void publishPacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();

        List<MqttProperties> properties=Arrays.asList(
                new MqttProperties(MqttProperty.PAYLOAD_FORMAT_INDICATOR,(byte)0),
                new MqttProperties(MqttProperty.MESSAGE_EXPIRY_INTERVAL, 456L),
                new MqttProperties(MqttProperty.TOPIC_ALIAS,21),
                new MqttProperties(MqttProperty.RESPONSE_TOPIC,"qwes"),
                new MqttProperties(MqttProperty.CORRELATION_DATA,Unpooled.copiedBuffer("qq".getBytes(StandardCharsets.UTF_8))),
                new MqttProperties(MqttProperty.USER_PROPERTY,new StringPair("wqe","2312")),
                new MqttProperties(MqttProperty.SUBSCRIPTION_IDENTIFIER,4561),
                new MqttProperties(MqttProperty.CONTENT_TYPE,"123456"));

        MqttPublishPacket packet=new MqttPublishPacket(FixedHeader.PUBLISH(true,MqttQoS.EXACTLY_ONCE,true),
                "/topic/test",30,properties,Unpooled.copiedBuffer(new byte[268435300]));

        assertCodec(mqttEncoder,mqttDecoder,packet,v->{
            ((ByteBuf)packet.getProperties().get(4).getValue()).resetReaderIndex();
            packet.getPayload().resetReaderIndex();
        });

    }

    @Test
    public void papaPacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();

        List<MqttProperties> properties=Arrays.asList(
                new MqttProperties(MqttProperty.REASON_STRING, "ewqew"),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "32142")),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "321awew42")));


        MqttPapaPacket packet=new MqttPapaPacket(FixedHeader.PUBACK,12,ReasonCode.SUCCESS,properties);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);
        packet=new MqttPapaPacket(FixedHeader.PUBREC,12,ReasonCode.SUCCESS,properties);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);
        packet=new MqttPapaPacket(FixedHeader.PUBREL,12,ReasonCode.SUCCESS,properties);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);
        packet=new MqttPapaPacket(FixedHeader.PUBCOMP,12,ReasonCode.SUCCESS,properties);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);

        packet=new MqttPapaPacket(FixedHeader.PINGREQ,0,null,null);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);
        packet=new MqttPapaPacket(FixedHeader.PINGRESP,0,null,null);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);

        packet=new MqttPapaPacket(FixedHeader.DISCONNECT,0,ReasonCode.SUCCESS,properties);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);
        packet=new MqttPapaPacket(FixedHeader.AUTH,0,ReasonCode.SUCCESS,properties);
        assertCodec(mqttEncoder,mqttDecoder,packet,null);
    }


    @Test
    public void subscribePacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();


        List<MqttProperties> properties=Arrays.asList(
                new MqttProperties(MqttProperty.SUBSCRIPTION_IDENTIFIER,35461),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "32142")),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "321awew42")));

        List<MqttSubscription> mqttSubscriptions = Arrays.asList(
                new MqttSubscription("/etset/#", MqttQoS.EXACTLY_ONCE, true, true, RetainHandling.SEND_MESSAGES_AT_THE_TIME),
                new MqttSubscription("/etsetwqe/+/#", MqttQoS.AT_MOST_ONCE, false, false, RetainHandling.DO_NOT_SEND));

        MqttSubscribePacket packet = new MqttSubscribePacket(FixedHeader.SUBSCRIBE, 20, properties, mqttSubscriptions);

        assertCodec(mqttEncoder,mqttDecoder,packet,v->{

        });

    }


    @Test
    public void unSubscribePacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();


        List<MqttProperties> properties=Arrays.asList(
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "32142")),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "321awew42")));

        List<String> mqttSubscriptions = Arrays.asList("dwe","dasgw");

        MqttUnSubscribePacket packet = new MqttUnSubscribePacket(FixedHeader.UNSUBSCRIBE, 20, properties, mqttSubscriptions);

        assertCodec(mqttEncoder,mqttDecoder,packet,v->{

        });

    }
    @Test
    public void unsubOrSubAckPacketTest(){
        MqttEncoder mqttEncoder = new MqttEncoder(new MqttSessionOption());
        MqttDecoder mqttDecoder = new MqttDecoder();


        List<MqttProperties> properties=Arrays.asList(
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "32142")),
                new MqttProperties(MqttProperty.USER_PROPERTY, new StringPair("123", "321awew42")));

        List<ReasonCode> reasonCodes = Arrays.asList(
                ReasonCode.SUCCESS,
                ReasonCode.NOT_AUTHORIZED);

        MqttUnsubOrSubAckPacket packet = new MqttUnsubOrSubAckPacket(FixedHeader.SUBACK, 20, properties, reasonCodes);

        assertCodec(mqttEncoder,mqttDecoder,packet,v->{

        });

    }



    private void assertCodec(MqttEncoder mqttEncoder, MqttDecoder mqttDecoder, MqttPacket packet, Handler<Void> beforeEq){
        List<Object> list=new ArrayList<>();
        mqttEncoder.encode(packet,list);
        Assertions.assertTrue(!list.isEmpty());
        mqttDecoder.decode(((ByteBuf) list.remove(0)).writeByte(1<<4),list);
        Assertions.assertTrue(!list.isEmpty());
        if (beforeEq!=null)
            beforeEq.handle(null);
        System.out.println(list.get(0));
        System.out.println(packet);
        System.out.println();
        Assertions.assertEquals(list.get(0),packet);
    }
}
