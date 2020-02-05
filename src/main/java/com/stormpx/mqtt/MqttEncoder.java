package com.stormpx.mqtt;

import com.stormpx.ex.PacketTooLagerException;
import com.stormpx.ex.ProtocolErrorException;
import com.stormpx.kit.MqttCodecUtil;
import com.stormpx.mqtt.packet.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.EncoderException;
import io.netty.handler.codec.MessageToMessageEncoder;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;

import java.util.List;

public class MqttEncoder extends MessageToMessageEncoder<MqttPacket> {
    private final static byte[] PROTOCOL_NAME_BYTES="MQTT".getBytes(CharsetUtil.UTF_8);


    private MqttSessionOption mqttSessionOption;

    public MqttEncoder(MqttSessionOption mqttSessionOption) {
        this.mqttSessionOption = mqttSessionOption;
    }

    public void encode(MqttPacket packet, List<Object> out){
        ByteBuf byteBuf = encodePacket(UnpooledByteBufAllocator.DEFAULT, packet);
        long maxPacketSize=mqttSessionOption.getEndPointMaximumPacketSize();
        if (byteBuf.readableBytes()>maxPacketSize)
            throw new PacketTooLagerException();

        out.add(byteBuf);
    }


    @Override
    protected void encode(ChannelHandlerContext ctx, MqttPacket packet, List<Object> out) throws Exception {

        ByteBuf byteBuf = encodePacket(ctx.alloc(), packet);
        long maxPacketSize=mqttSessionOption.getEndPointMaximumPacketSize();
        if (byteBuf.readableBytes()>maxPacketSize)
            throw new PacketTooLagerException();

        out.add(byteBuf);
    }


    private ByteBuf encodePacket(ByteBufAllocator alloc,MqttPacket packet){
        switch (packet.fixedHeader().getPacketType()){

            case CONNECT:
                return encodeConnectPacket(alloc, (MqttConnectPacket) packet);
            case CONNACK:
                return encodeConnAckPacket(alloc, (MqttConnAckPacket) packet);
            case PUBLISH:
                return encodePublishPacket(alloc, (MqttPublishPacket) packet);
            case PUBACK:
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
            case DISCONNECT:
            case AUTH:
            case PINGREQ:
            case PINGRESP:
                return encodePapaPacket(alloc, (MqttPapaPacket) packet);
            case SUBSCRIBE:
                return encodeSubscribePacket(alloc, (MqttSubscribePacket) packet);
            case SUBACK:
                return encodeSubAckPacket(alloc, (MqttUnsubOrSubAckPacket) packet);
            case UNSUBACK:
                return encodeUnsubAckPacket(alloc, (MqttUnsubOrSubAckPacket) packet);
            case UNSUBSCRIBE:
                return encodeUnSubscribePacket(alloc, (MqttUnSubscribePacket) packet);
        }
        throw new EncoderException("unknown packet type"+packet.fixedHeader().getPacketType());
    }

    private ByteBuf encodeConnectPacket(ByteBufAllocator alloc,MqttConnectPacket packet){
        int fixedHeaderLength=0;
        int variableHeaderLength=0;
        int payloadLength=0;


        byte[] protocolNameBytes = PROTOCOL_NAME_BYTES;
        variableHeaderLength+=2;
        variableHeaderLength+=protocolNameBytes.length;

        MqttVersion version = packet.getVersion();
        byte protocolLevel = version.protocolLevel();
        variableHeaderLength+=1;
        //encode connectFlags
        byte connectFlags=0;
        variableHeaderLength+=1;
        if (version==MqttVersion.MQTT_3_1_1){
            if (packet.getUserName()==null&&packet.getPassword()!=null){
                throw new EncoderException("userName set 0 password must set 0");
            }
        }
        if (packet.getUserName()!=null){
            connectFlags|=0x80;
            payloadLength+=2;
            payloadLength+= MqttCodecUtil.encodeUtf8String(packet.getUserName()).length;
        }
        if (packet.getPassword()!=null){
            connectFlags|=0x40;
            payloadLength+=2;
            payloadLength+= packet.getPassword().readableBytes();
        }
        if (packet.isWillFlag()){
            connectFlags|=0x04;
            if (packet.isWillRetain()){
                connectFlags|=0x20;
            }
            connectFlags|=packet.getWillQos().value()<<3;
        }
        if (packet.isCleanStart()){
            connectFlags|=0x02;
        }

        variableHeaderLength+=2;
        int keepAlive = packet.getKeepAlive();

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(ControlPacketType.CONNECT,packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }


        if (packet.getClientIdentifier()==null){
            throw new EncoderException(" Client Identifier is null");
        }

        byte[] clientIdentifierBytes =  MqttCodecUtil.encodeUtf8String(packet.getClientIdentifier());
        payloadLength+=2;
        payloadLength+=clientIdentifierBytes.length;

        PropertiesEncoder willProperties=null;
        if (packet.isWillFlag()){
            if (packet.getWillTopic()==null||packet.getWillPayload()==null||packet.getWillQos()==null)
                throw new ProtocolErrorException();
            if (packet.getWillProperties()!=null) {
                //will properties
                willProperties = new PropertiesEncoder(ControlPacketType.CONNECT,packet.getWillProperties());
                payloadLength+= MqttCodecUtil.encodeVariableLength(willProperties.getLength()).length;
                payloadLength+=willProperties.getLength();
            }
            //will topic
            payloadLength+=2;
            payloadLength+= MqttCodecUtil.encodeUtf8String(packet.getWillTopic()).length;
            //will payload
            payloadLength+=2;
            payloadLength+=packet.getWillPayload().readableBytes();
        }

        FixedHeader fixedHeader = packet.fixedHeader();
        byte bit = encodePacketBit(fixedHeader);
        fixedHeaderLength+=1;
        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength + payloadLength);
        fixedHeaderLength+=remainingLengthBytes.length;
        //write buf
        ByteBuf buf = alloc.buffer(fixedHeaderLength + variableHeaderLength + payloadLength);

        //fixedHeader
        writeFixedHeader(buf,bit,remainingLengthBytes);

        //variableHeader
        buf.writeShort(protocolNameBytes.length);
        buf.writeBytes(protocolNameBytes);
        buf.writeByte(protocolLevel);
        buf.writeByte(connectFlags);
        buf.writeShort(keepAlive);
        writeProperties(buf, propertiesEncoder);

        //payload
        buf.writeShort(clientIdentifierBytes.length);
        buf.writeBytes(clientIdentifierBytes);
        if (packet.isWillFlag()){
            writeProperties(buf,willProperties);
            byte[] topic =  MqttCodecUtil.encodeUtf8String(packet.getWillTopic());
            writeUtf8String(buf,topic);

            buf.writeShort(packet.getWillPayload().readableBytes());
            buf.writeBytes(packet.getWillPayload());
        }
        if (packet.getUserName()!=null){
            byte[] userName =  MqttCodecUtil.encodeUtf8String(packet.getUserName());
            writeUtf8String(buf,userName);
        }
        if (packet.getPassword()!=null){
            ByteBuf password = packet.getPassword();
            buf.writeShort(password.readableBytes());
            buf.writeBytes(password);
        }

        return buf;
    }

    private ByteBuf encodeConnAckPacket(ByteBufAllocator alloc, MqttConnAckPacket packet){
        int fixedHeaderLength=0;
        int variableHeaderLength=0;

        variableHeaderLength+=1;
        byte connectAcknowledgeFlags=0;
        if (packet.isSessionPresent()){
            connectAcknowledgeFlags|=0x01;
        }

        variableHeaderLength+=1;
        byte reasonCode=packet.getCode();

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(ControlPacketType.CONNACK,packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        long maxPacketSize=mqttSessionOption.getEndPointMaximumPacketSize();

        variableHeaderLength=checkAndReducePacketSize(variableHeaderLength,maxPacketSize, propertiesEncoder);

        byte bit = encodePacketBit(packet.fixedHeader());
        fixedHeaderLength+=1;
        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength );
        fixedHeaderLength+=remainingLengthBytes.length;


        //write buf
        ByteBuf buf = alloc.buffer(fixedHeaderLength + variableHeaderLength);

        //fixedHeader
        writeFixedHeader(buf,bit,remainingLengthBytes);

        //variableHeader
        buf.writeByte(connectAcknowledgeFlags);
        buf.writeByte(reasonCode);
        writeProperties(buf, propertiesEncoder);
        return buf;
    }


    private ByteBuf encodePublishPacket(ByteBufAllocator alloc, MqttPublishPacket packet){
        int variableHeaderLength=0;
        int payloadLength=0;

        if (packet.getTopicName()==null||packet.getTopicName().isBlank()){
            if (packet.getProperties()==null||
                    packet.getProperties().stream().anyMatch(mqttProperties -> mqttProperties.getProperty()==MqttProperty.TOPIC_ALIAS)){
                throw new EncoderException("");
            }
        }

        byte[] topic =  MqttCodecUtil.encodeUtf8String(packet.getTopicName());
        variableHeaderLength+=2;
        variableHeaderLength+=topic.length;

        if (packet.fixedHeader().getQosAsEnum()!= MqttQoS.AT_MOST_ONCE){
            if (packet.getPacketIdentifier()==0)
                throw new EncoderException("Packet Identifier must non zero");
            variableHeaderLength+=2;
        }

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(ControlPacketType.PUBLISH,packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        payloadLength+=packet.getPayload().readableBytes();

        byte bit = encodePacketBit(packet.fixedHeader());
        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength + payloadLength);
        //write buf
        ByteBuf buf = alloc.buffer(1+remainingLengthBytes.length + variableHeaderLength+payloadLength);

        //fixedHeader
        writeFixedHeader(buf,bit,remainingLengthBytes);

        //variableHeader
        writeUtf8String(buf,topic);
        if (packet.fixedHeader().getQosAsEnum()!=MqttQoS.AT_MOST_ONCE){
            buf.writeShort(packet.getPacketIdentifier());
        }
        writeProperties(buf, propertiesEncoder);
        //payload
        buf.writeBytes(packet.getPayload());
        return buf;
    }

    private ByteBuf encodePapaPacket(ByteBufAllocator alloc, MqttPapaPacket packet){
        int variableHeaderLength=0;

        if (packet.getPacketIdentifier()!=0){
            variableHeaderLength+=2;
        }
        if (packet.getReasonCode()!=null){
            variableHeaderLength+=1;
        }
        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(packet.fixedHeader().getPacketType(),packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        long maxPacketSize=mqttSessionOption.getEndPointMaximumPacketSize();

        variableHeaderLength=checkAndReducePacketSize(variableHeaderLength,maxPacketSize, propertiesEncoder);

        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength);
        //write buf
        ByteBuf buf = alloc.buffer(1+remainingLengthBytes.length + variableHeaderLength);

        writeFixedHeader(buf,encodePacketBit(packet.fixedHeader()),remainingLengthBytes);

        if (packet.getPacketIdentifier()!=0){
            buf.writeShort(packet.getPacketIdentifier());
        }
        if (packet.getReasonCode()!=null){
            buf.writeByte(packet.getReasonCode().byteValue());
        }

        writeProperties(buf, propertiesEncoder);
        return buf;
    }

    private ByteBuf encodeSubscribePacket(ByteBufAllocator alloc,MqttSubscribePacket packet){

        int variableHeaderLength=0;
        int payloadLength=0;

        int packetIdentifier = packet.getPacketIdentifier();
        variableHeaderLength+=2;

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(packet.fixedHeader().getPacketType(),packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        List<MqttSubscription> subscriptions = packet.getSubscriptions();
        if (subscriptions==null||subscriptions.isEmpty())
            throw new EncoderException("must contain at least one Topic Filter");
        for (MqttSubscription subscription : subscriptions) {
            byte[] topic = MqttCodecUtil.encodeUtf8String(subscription.getTopicFilter());
            payloadLength+=2;
            payloadLength+=topic.length;
            payloadLength+=1;
        }

        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength+payloadLength);
        //write buf
        ByteBuf buf = alloc.buffer(1+remainingLengthBytes.length + variableHeaderLength+payloadLength);

        writeFixedHeader(buf,encodePacketBit(packet.fixedHeader()),remainingLengthBytes);

        buf.writeShort(packetIdentifier);
        writeProperties(buf, propertiesEncoder);

        for (MqttSubscription subscription : subscriptions) {
            writeUtf8String(buf,MqttCodecUtil.encodeUtf8String(subscription.getTopicFilter()));
            byte options=(byte)subscription.getQos().value();
            if (subscription.isNoLocal()){
                options|=0x04;
            }
            if (subscription.isRetainAsPublished()){
                options|=0x08;
            }
            RetainHandling retainHandling = subscription.getRetainHandling();
            if (retainHandling !=null){
                options|=(retainHandling.getValue()<<4);
            }
            buf.writeByte(options);
        }
        return buf;
    }

    private ByteBuf encodeSubAckPacket(ByteBufAllocator alloc, MqttUnsubOrSubAckPacket packet){
        int variableHeaderLength=0;
        int payloadLength=0;

        int packetIdentifier = packet.getPacketIdentifier();
        variableHeaderLength+=2;

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(packet.fixedHeader().getPacketType(),packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        List<ReasonCode> reasonCodes = packet.getReasonCodes();
        if (reasonCodes==null||reasonCodes.isEmpty())
            throw new EncoderException("payload must contain at least one code");
        variableHeaderLength+=reasonCodes.size();

        long maxPacketSize=mqttSessionOption.getEndPointMaximumPacketSize();

        variableHeaderLength=checkAndReducePacketSize(variableHeaderLength,maxPacketSize, propertiesEncoder);

        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength+payloadLength);
        //write buf
        ByteBuf buf = alloc.buffer(1+remainingLengthBytes.length + variableHeaderLength+payloadLength);

        writeFixedHeader(buf,encodePacketBit(packet.fixedHeader()),remainingLengthBytes);

        buf.writeShort(packetIdentifier);
        writeProperties(buf, propertiesEncoder);

        for (ReasonCode reasonCode : reasonCodes) {
            buf.writeByte(reasonCode.byteValue());
        }

        return buf;
    }

    private ByteBuf encodeUnSubscribePacket(ByteBufAllocator alloc,MqttUnSubscribePacket packet){

        int variableHeaderLength=0;
        int payloadLength=0;

        int packetIdentifier = packet.getPacketIdentifier();
        variableHeaderLength+=2;

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(packet.fixedHeader().getPacketType(),packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        List<String> subscriptions = packet.getSubscriptions();
        if (subscriptions==null||subscriptions.isEmpty())
            throw new EncoderException("must contain at least one Topic Filter");

        for (String subscription : subscriptions) {
            byte[] topic = MqttCodecUtil.encodeUtf8String(subscription);
            payloadLength+=2;
            payloadLength+=topic.length;
        }

        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength+payloadLength);
        //write buf
        ByteBuf buf = alloc.buffer(1+remainingLengthBytes.length + variableHeaderLength+payloadLength);

        writeFixedHeader(buf,encodePacketBit(packet.fixedHeader()),remainingLengthBytes);

        buf.writeShort(packetIdentifier);
        writeProperties(buf, propertiesEncoder);

        for (String subscription : subscriptions) {
            writeUtf8String(buf,MqttCodecUtil.encodeUtf8String(subscription));
        }

        return buf;
    }

    /**
     * if mqtt version==3.1.1 reason code must null
     * @param alloc
     * @param packet
     * @return
     */
    private ByteBuf encodeUnsubAckPacket(ByteBufAllocator alloc, MqttUnsubOrSubAckPacket packet){
        int variableHeaderLength=0;
        int payloadLength=0;

        int packetIdentifier = packet.getPacketIdentifier();
        variableHeaderLength+=2;

        PropertiesEncoder propertiesEncoder =null;
        if (packet.getProperties()!=null){
            propertiesEncoder =new PropertiesEncoder(packet.fixedHeader().getPacketType(),packet.getProperties());
            variableHeaderLength+= MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()).length;
            variableHeaderLength+= propertiesEncoder.getLength();
        }

        List<ReasonCode> reasonCodes = packet.getReasonCodes();
        if (reasonCodes!=null)
            variableHeaderLength+=reasonCodes.size();

        long maxPacketSize=mqttSessionOption.getEndPointMaximumPacketSize();

        variableHeaderLength=checkAndReducePacketSize(variableHeaderLength,maxPacketSize, propertiesEncoder);

        byte[] remainingLengthBytes =  MqttCodecUtil.encodeVariableLength(variableHeaderLength+payloadLength);
        //write buf
        ByteBuf buf = alloc.buffer(1+remainingLengthBytes.length + variableHeaderLength+payloadLength);

        writeFixedHeader(buf,encodePacketBit(packet.fixedHeader()),remainingLengthBytes);

        buf.writeShort(packetIdentifier);
        writeProperties(buf, propertiesEncoder);

        if (reasonCodes!=null) {
            for (ReasonCode reasonCode : reasonCodes) {
                buf.writeByte(reasonCode.byteValue());
            }
        }
        return buf;
    }

    private void writeUtf8String(ByteBuf buf,byte[] strBytes){
        buf.writeShort(strBytes.length);
        buf.writeBytes(strBytes);
    }

    private void writeFixedHeader(ByteBuf buf,byte firstBit,byte[] remainingLength){
        buf.writeByte(firstBit);
        buf.writeBytes(remainingLength);
    }

    private void writeProperties(ByteBuf buf, PropertiesEncoder propertiesEncoder){
        if (propertiesEncoder !=null){
            buf.writeBytes( MqttCodecUtil.encodeVariableLength(propertiesEncoder.getLength()));
            propertiesEncoder.encode(buf);
        }
    }


    private byte encodePacketBit(FixedHeader fixedHeader){
        byte b=0;
        b|=fixedHeader.getPacketType().value()<<4;
        if (fixedHeader.isDup()){
            b|=0x08;
        }
        b|=fixedHeader.getQosAsEnum().value()<<1;
        if (fixedHeader.isRetain()){
            b|=0x01;
        }
        return b;
    }

    private int checkAndReducePacketSize(int variableHeaderLength, long maxPacketSize, PropertiesEncoder propertiesEncoder){
        if (propertiesEncoder ==null)return variableHeaderLength;
        if (variableHeaderLength>maxPacketSize){
            for (MqttProperties optionalProperties : propertiesEncoder.getOptionalProperties()) {
                variableHeaderLength-= propertiesEncoder.getLength();
                propertiesEncoder.del(optionalProperties);
                variableHeaderLength+= propertiesEncoder.getLength();
                if (variableHeaderLength<=maxPacketSize){
                    break;
                }
            }
        }
        if (variableHeaderLength>maxPacketSize){
            throw new PacketTooLagerException();
        }
        return variableHeaderLength;
    }


}
