package com.stormpx.server;

import com.stormpx.ex.QosNotSupportedException;
import com.stormpx.ex.RetainNotSupportedException;
import com.stormpx.message.MqttBrokerMessage;
import com.stormpx.message.MqttPublishMessage;
import com.stormpx.message.MqttSubscribeMessage;
import com.stormpx.message.MqttUnSubscribeMessage;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.ControlPacketType;
import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.ReasonCode;
import com.stormpx.mqtt.packet.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class Mqtt3Context extends AbstractMqttContext {

    public Mqtt3Context(MqttSocket mqttSocket, MqttConnectPacket connectPacket) {
        super(mqttSocket, connectPacket);
    }

    @Override
    protected MqttPublishMessage getPublishMessage(MqttPublishPacket publishPacket){
        FixedHeader fixedHeader = publishPacket.fixedHeader();
        if ((fixedHeader.isRetain()&&!mqttSessionOption.isRetainAvailable())){
            throw new RetainNotSupportedException();
        }
        if (fixedHeader.getQos()>mqttSessionOption.getMaxQos().value()){
            throw new QosNotSupportedException();
        }
        MqttPublishMessage mqttPublishMessage =  new MqttPublishMessage(publishPacket.getTopicName(), fixedHeader.getQosAsEnum(), fixedHeader.isRetain(),
                fixedHeader.isDup(), Buffer.buffer(publishPacket.getPayload()), publishPacket.getPacketIdentifier());
        return mqttPublishMessage;
    }

    @Override
    public void handleException(Throwable throwable) {
        super.handleException(throwable);
        if (isClose())
            return;

        if (!connectAck) {
            MqttPacket packet = new MqttConnAckPacket(FixedHeader.CONNACK,
                     MqttConnectReturnCode.CONNECTION_REFUSED_SERVER_UNAVAILABLE.byteValue(), null,false);
            mqttSocket.writePacket(packet);
            connectAck=true;
        }
        //disconnect
        close();

    }

    @Override
    protected void handleSubscribe(MqttSubscribePacket subscribePacket) {
        if (mqttSession.containsPacketId(subscribePacket.getPacketIdentifier())){
            subscribeAcknowledge(subscribePacket.getPacketIdentifier(),subscribePacket.getSubscriptions().stream().map(s->ReasonCode.UNSPECIFIED_ERROR).collect(Collectors.toList()),null,null);
            return;
        }
        MqttSubscribeMessage subscribeMessage =
                new MqttSubscribeMessage(subscribePacket.getSubscriptions(), subscribePacket.getPacketIdentifier(), 0, Collections.emptyList());

        if (subscribeHandler != null) {
            subscribeHandler.handle(subscribeMessage);
        }
    }

    @Override
    protected void handleUnSubscribe(MqttUnSubscribePacket unSubscribePacket) {
        if (mqttSession.containsPacketId(unSubscribePacket.getPacketIdentifier())){
            subscribeAcknowledge(unSubscribePacket.getPacketIdentifier(),unSubscribePacket.getSubscriptions().stream().map(s->ReasonCode.UNSPECIFIED_ERROR).collect(Collectors.toList()),null,null);
            return;
        }
        MqttUnSubscribeMessage mqttUnSubscribeMessage =
                new MqttUnSubscribeMessage(unSubscribePacket.getSubscriptions(), unSubscribePacket.getPacketIdentifier(), Collections.emptyList());
        if (unSubscribeHandler!=null){
            unSubscribeHandler.handle(mqttUnSubscribeMessage);
        }
    }

    @Override
    protected void accept0(boolean sessionPresent) {
        if (connectAck||close)
            return;
        MqttConnAckPacket packet=new MqttConnAckPacket(FixedHeader.CONNACK,
                MqttConnectReturnCode.CONNECTION_ACCEPTED.byteValue(),null,sessionPresent);
        mqttSocket.writePacket(packet);
    }

    @Override
    protected Future<Void> writeMessage(MqttBrokerMessage message) {
        ByteBuf payload= Unpooled.copiedBuffer(message.getPayload().getByteBuf());

        MqttPublishPacket publishPacket = new MqttPublishPacket(new FixedHeader(ControlPacketType.PUBLISH, message.isDup(), message.getQos().value(), message.isRetain(), 0),
                message.getTopic(), message.getPacketId(), null, payload);

        Promise<Void> promise = Promise.promise();

        mqttSocket.writePacket(publishPacket,promise);

        return promise.future();
    }

    @Override
    protected void handlePublishReceive(MqttPapaPacket pubRecPacket) {
//       publishRelease(pubRecPacket.getPacketIdentifier(),ReasonCode.SUCCESS,null,null);
        super.handlePublishReceive(pubRecPacket);
    }

    @Override
    public MqttContext reject(byte code) {

        MqttConnectReturnCode mqttConnectReturnCode = MqttConnectReturnCode.valueOf(code);

        MqttConnAckPacket packet=new MqttConnAckPacket(FixedHeader.CONNACK,
                mqttConnectReturnCode.byteValue(),null,false);
        connectAck=true;
       mqttSocket.writePacket(packet,ar->close());
        if (logger.isDebugEnabled())
            logger.debug("reject client :{} by code:{}",mqttSession.clientIdentifier(),code);
        return this;
    }

    @Override
    public MqttContext reject(boolean permanent, String serverReference) {
        logger.info("mqtt v3.1.1 don't have serverReference");
        close();
        return this;
    }

    @Override
    public void publishAcknowledge(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {

        mqttSocket.writePacket(new MqttPapaPacket(FixedHeader.PUBACK,
                packetId, null, null));
    }

    @Override
    public void publishReceived(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {

        mqttSocket.writePacket(new MqttPapaPacket(FixedHeader.PUBREC,
                packetId, null, null));
    }

    @Override
    public void publishRelease(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        mqttSocket.writePacket(new MqttPapaPacket(FixedHeader.PUBREL,
                packetId, null, null));
        super.publishRelease(packetId, reasonCode, userProperty, reasonString);
    }

    @Override
    public void publishComplete(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        mqttSocket.writePacket(new MqttPapaPacket(FixedHeader.PUBCOMP,
                packetId, null, null));
    }

    @Override
    public void subscribeAcknowledge(int packetId, List<ReasonCode> reasonCodes, List<StringPair> userProperty, String reasonString) {
        List<ReasonCode> list = reasonCodes.stream().map(reasonCode -> {
            if (reasonCode.byteValue() > 0x02)
                return ReasonCode.UNSPECIFIED_ERROR;
            return reasonCode;
        }).collect(Collectors.toList());
        MqttUnsubOrSubAckPacket packet= new MqttUnsubOrSubAckPacket(FixedHeader.SUBACK,
                packetId,null,list);
        mqttSocket.writePacket(packet);
    }

    @Override
    public void unSubscribeAcknowledge(int packetId, List<ReasonCode> reasonCodes, List<StringPair> userProperty, String reasonString) {
        MqttUnsubOrSubAckPacket packet= new MqttUnsubOrSubAckPacket(FixedHeader.UNSUBACK,
                packetId,null,null);
        mqttSocket.writePacket(packet);
    }
}
