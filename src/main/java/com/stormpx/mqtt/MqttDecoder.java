package com.stormpx.mqtt;

import com.stormpx.ex.MalformedPacketException;
import com.stormpx.ex.ProtocolErrorException;
import com.stormpx.kit.StringPair;
import com.stormpx.kit.TopicUtil;
import com.stormpx.mqtt.packet.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.netty.util.CharsetUtil;
import io.vertx.core.buffer.Buffer;

import java.util.*;

import static com.stormpx.mqtt.MqttProperty.*;

public class MqttDecoder extends ByteToMessageDecoder {
    private FixedHeader fixedHeader;
    private MqttVersion version;
    private Throwable cause;

    /**
     *
     * @param in
     * @param out
     * @return expect next length
     */
    public void decode( ByteBuf in, List<Object> out)  {
        try {
            decode(null,in,out);
        } catch (Exception e) {
            //ignore
        }
    }

    @Override
    protected void decode(ChannelHandlerContext ctx, ByteBuf in, List<Object> out) throws Exception {
        try {
            if (cause!=null){
                in.skipBytes(in.readableBytes());
                return;
            }
            if (fixedHeader==null){
                if (in.readableBytes()<2){
                    return;
                }
                in.markReaderIndex();
                this.fixedHeader = decodeFixedHeader(in);
                if (fixedHeader==null){
                    in.resetReaderIndex();
                    return;
                }
                in.markReaderIndex();
                // for remaining length 0 packet
                decode(ctx, in, out);

            }else{
                if (fixedHeader.getRemainingLength()>in.readableBytes()){
                    return;
                }
                int readableBytes = in.readableBytes();
                Object o = decodeVariableHeader(in);
                if (readableBytes-in.readableBytes()!=fixedHeader.getRemainingLength()){
                    throw new ProtocolErrorException("consumed bytes "+ (readableBytes-in.readableBytes()) +" != remainingLength "+fixedHeader.getRemainingLength());
                }
                out.add(o);
                resetState();
            }
        } catch (Throwable e) {
            this.cause=e;
            out.add(new MqttInvalidPacket(fixedHeader,e));
        }
    }

    private void resetState(){
        this.fixedHeader=null;
    }


    private FixedHeader decodeFixedHeader(ByteBuf buf){
       /* for (int i = 1; i <= 5; i++) {
            if (buf.readableBytes()<i)
                return null;
            if ((buf.getByte(buf.readerIndex()+i)&128)==0){
                break;
            }else if (i==5){
                throw new MalformedPacketException("Malformed Variable Byte Integer");
            }
        }*/
        short b=buf.readUnsignedByte();
        ControlPacketType packetType = ControlPacketType.valueOf((b >> 4));;
        boolean dup=(b&0x08)!=0;
        int qos = (b & 0x06)>> 1;
        boolean retain= (b&0x01)!=0;

        int length=decodeVariableLength(buf);
        if (length==-1)
            return null;

        FixedHeader fixedHeader = new FixedHeader(packetType, dup, qos, retain, length);
        verifyFixedHeader(fixedHeader);
        return fixedHeader;
    }

    private void verifyFixedHeader(FixedHeader fixedHeader){
        switch (fixedHeader.getPacketType()){
            case PUBLISH:
                if (fixedHeader.getQos()>2)
                    throw new DecoderException("");
                break;
            case PUBREL:
                if (fixedHeader.getQos()!=1)
                    throw new DecoderException("Protocol Error");
                break;
            case SUBSCRIBE:
                if (fixedHeader.getQos()!=1)
                    throw new DecoderException("Protocol Error");
                break;
            case UNSUBSCRIBE:
                if (fixedHeader.getQos()!=1)
                    throw new DecoderException("Protocol Error");
                break;
            case CONNECT:
            case CONNACK:
            case PUBACK:
            case PUBREC:
            case PUBCOMP:
            case SUBACK:
            case UNSUBACK:
            case PINGREQ:
            case PINGRESP:
            case DISCONNECT:
                if (fixedHeader.isDup()||fixedHeader.isRetain()||fixedHeader.getQos()!=0)
                    throw new DecoderException("Protocol Error");
        }
    }

    private Object decodeVariableHeader(ByteBuf buf){
        switch (fixedHeader.getPacketType()){
            case CONNECT:
                return decodeConnect(buf);
            case CONNACK:
                return decodeConnAck(buf);
            case PUBLISH:
                return decodePublish(buf);
            case PUBACK:
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
            case DISCONNECT:
            case AUTH:
            case PINGREQ:
            case PINGRESP:
                return decodePapaPacket(buf);
            case SUBSCRIBE:
                return decodeSubscribe(buf);
            case SUBACK:
            case UNSUBACK:
                return decodeUnsubOrSubAck(buf);
            case UNSUBSCRIBE:
                return decodeUnSubscribe(buf);



        }
        throw new ProtocolErrorException();
    }

    /**
     * decode CONNECT packet VariableHeader & payload
     * @param buf
     * @return MqttConnectPacket
     */
    private MqttPacket decodeConnect(ByteBuf buf){

        String mqtt = readUtf8String(buf);
        if (!mqtt.equalsIgnoreCase("mqtt")){
            throw new ProtocolErrorException("protocol name "+mqtt+" != mqtt");
        }
        byte protocolVersion = buf.readByte();
        this.version = MqttVersion.valueOf(protocolVersion);
        byte connectFlags = buf.readByte();

        boolean userNameFlag=(connectFlags&0x80)==0x80;
        boolean passwordFlag=(connectFlags&0x40)==0x40;
        boolean willFlag= (connectFlags&0x04)==0x04;
        boolean willRetain=(connectFlags&0x20)==0x20;
        MqttQoS willQos= MqttQoS.valueOf((connectFlags&0x18)>>3);
        if (!willFlag){
            if (willQos!=MqttQoS.AT_MOST_ONCE||willRetain){
                throw new MalformedPacketException();
            }
        }

        boolean cleanStart= (connectFlags&0x02)==0x02;
        boolean reserved= (connectFlags&0x01)==0x01;
        if (reserved)
            throw new MalformedPacketException();

        if (version==MqttVersion.MQTT_3_1_1){
             if (!userNameFlag&&passwordFlag){
                throw new ProtocolErrorException();
            }
        }
        int keepAlive=buf.readUnsignedShort();
        List<MqttProperties> properties=decodeProperties(fixedHeader.getPacketType(),buf);
        if (properties!=null) {

            for (MqttProperties property : properties) {
                MqttProperty mqttProperty = property.getProperty();
                if (mqttProperty == WILL_DELAY_INTERVAL || mqttProperty == PAYLOAD_FORMAT_INDICATOR || mqttProperty == CONTENT_TYPE
                        || mqttProperty == RESPONSE_TOPIC || mqttProperty == CORRELATION_DATA || mqttProperty == MESSAGE_EXPIRY_INTERVAL) {
                    throw new ProtocolErrorException(mqttProperty + "is not valid for CONNECT packet");
                }
            }
        }
        String clientIdentifier = readUtf8String(buf);
        List<MqttProperties> willProperties = Collections.emptyList();
        String willTopic=null;
        ByteBuf willPayload=null;
        if (willFlag){
            willProperties = decodeProperties(fixedHeader.getPacketType(),buf);
            if (willProperties!=null) {
                for (MqttProperties willProperty : willProperties) {
                    MqttProperty mqttProperty = willProperty.getProperty();
                    if (mqttProperty == SESSION_EXPIRY_INTERVAL || mqttProperty == AUTHENTICATION_METHOD || mqttProperty == AUTHENTICATION_DATA
                            || mqttProperty == REQUEST_PROBLEM_INFORMATION || mqttProperty == REQUEST_RESPONSE_INFORMATION
                            || mqttProperty == RECEIVE_MAXIMUM || mqttProperty == TOPIC_ALIAS_MAXIMUM || mqttProperty == MAXIMUM_PACKET_SIZE) {
                        throw new ProtocolErrorException(mqttProperty + "is not valid for will");
                    }
                }
            }
            willTopic=readUtf8String(buf);
            willPayload=readBinaryData(buf);
        }
        String userName=null;
        if (userNameFlag){
            userName=readUtf8String(buf);
        }
        ByteBuf password=null;
        if (passwordFlag) {
            password=readBinaryData(buf);
        }

        return new MqttConnectPacket(fixedHeader,version,clientIdentifier,cleanStart,keepAlive,properties, willFlag, willRetain,
                willProperties,willQos,willTopic,willPayload,userName,password);
    }

    /**
     * decode CONNACK packet variableHeader
     * @param buf
     * @return
     */
    private MqttPacket decodeConnAck(ByteBuf buf){
        byte connectAcknowledgeFlags=buf.readByte();

        byte reasonCode=buf.readByte();
        if (version==MqttVersion.MQTT_5_0){
            reasonCode=ReasonCode.valueOf(reasonCode, ControlPacketType.CONNACK).byteValue();
        }else{
            reasonCode= MqttConnectReturnCode.valueOf(reasonCode).byteValue();
        }
        List<MqttProperties> properties = decodeProperties(ControlPacketType.CONNACK, buf);
        boolean sessionPresent = (connectAcknowledgeFlags&0x01)==0x01;
        return new MqttConnAckPacket(fixedHeader,reasonCode,properties,sessionPresent);
    }

    /**
     * decode PUBLISH packet variableHeader & payload
     * @param buf
     * @return
     */
    private MqttPacket decodePublish(ByteBuf buf){
        int readableBytes = buf.readableBytes();
        String topicName = readUtf8String(buf);
        int packetIdentifier=0;
        if (fixedHeader.getQosAsEnum()!=MqttQoS.AT_MOST_ONCE){
            packetIdentifier=buf.readUnsignedShort();
            if (packetIdentifier==0)
                throw new ProtocolErrorException("must a non zero Packet Identifier");
        }
        List<MqttProperties> properties = decodeProperties(ControlPacketType.PUBLISH, buf);
        int length = fixedHeader.getRemainingLength() - (readableBytes - buf.readableBytes());
        if (length <0){
            throw new ProtocolErrorException();
        }

        ByteBuf payload= Unpooled.copiedBuffer(buf.readSlice(length));

        return new MqttPublishPacket(fixedHeader,topicName,packetIdentifier,properties,payload);
    }

    /**
     * decode SUBSCRIBE packet variableHeader & payload
     * @param buf
     * @return
     */
    private MqttPacket decodeSubscribe(ByteBuf buf){
        int readableBytes = buf.readableBytes();
        int packetIdentifier = buf.readUnsignedShort();
        List<MqttProperties>  properties=decodeProperties(ControlPacketType.SUBSCRIBE, buf);
        List<MqttSubscription> subscriptions=new ArrayList<>();
        while (readableBytes-buf.readableBytes()<fixedHeader.getRemainingLength()){
            String topicFilter = readUtf8String(buf);
            byte subscriptionOptions = buf.readByte();
            MqttQoS qos=MqttQoS.valueOf(subscriptionOptions&0x03);
            boolean noLocal = (subscriptionOptions & 0x04) == 0x04;
            if (TopicUtil.isShareTopicFilter(topicFilter)&&noLocal){
                throw new ProtocolErrorException("No Local bit to 1 on a Shared Subscription");
            }
            boolean retainAsPublished=(subscriptionOptions&0x08)==0x08;
            RetainHandling retainHandling=RetainHandling.valueOf((subscriptionOptions&0x30)>>4);

            subscriptions.add(new MqttSubscription(topicFilter,qos,noLocal,retainAsPublished,retainHandling));

        }
        if (subscriptions.isEmpty()){
            throw new ProtocolErrorException("must contain at least one Topic Filter and Subscription Options pair");
        }
        return new MqttSubscribePacket(fixedHeader,packetIdentifier,properties,subscriptions);
    }

    /**
     * decode SUBACK/UNSUBACK packet variableHeader & payload
     * @param buf
     * @return
     */
    private MqttPacket decodeUnsubOrSubAck(ByteBuf buf){
        int readableBytes = buf.readableBytes();
        int packetIdentifier = buf.readUnsignedShort();
        List<MqttProperties>  properties=decodeProperties(ControlPacketType.SUBACK, buf);
        List<ReasonCode> codes=new ArrayList<>();
        while (readableBytes-buf.readableBytes()<fixedHeader.getRemainingLength()){
            codes.add(ReasonCode.valueOf(buf.readByte(),ControlPacketType.SUBACK));
        }
        if (codes.isEmpty()){
            throw new ProtocolErrorException("reasonCode empty");
        }
        return new MqttUnsubOrSubAckPacket(fixedHeader,packetIdentifier,properties,codes);
    }


    /**
     * decode UNSUBSCRIBE packet variableHeader & payload
     * @param buf
     * @return
     */
    private MqttPacket decodeUnSubscribe(ByteBuf buf){
        int readableBytes = buf.readableBytes();
        int packetIdentifier = buf.readUnsignedShort();
        List<MqttProperties>  properties=decodeProperties(ControlPacketType.SUBSCRIBE, buf);
        List<String> subscriptions=new ArrayList<>();
        while (readableBytes-buf.readableBytes()<fixedHeader.getRemainingLength()){
            String topicFilter = readUtf8String(buf);
            subscriptions.add(topicFilter);
        }
        if (subscriptions.isEmpty()){
            throw new ProtocolErrorException("must contain at least one Topic Filter ");
        }
        return new MqttUnSubscribePacket(fixedHeader,packetIdentifier,properties,subscriptions);
    }


    /**
     * decode PUBACK PUBREC PUBREL PUBCOMP DISCONNECT AUTH variableHeader
     * @param buf
     * @return
     */
    private MqttPacket decodePapaPacket(ByteBuf buf){
        int readableBytes = buf.readableBytes();
        int packetIdentifier=0;
        ReasonCode reasonCode=null;
        List<MqttProperties> properties=null;
        switch (fixedHeader.getPacketType()){
            case PUBACK:
            case PUBREC:
            case PUBREL:
            case PUBCOMP:
                packetIdentifier=buf.readUnsignedShort();
            case DISCONNECT:
            case AUTH:
                if (version==MqttVersion.MQTT_3_1_1||readableBytes-buf.readableBytes()-fixedHeader.getRemainingLength()==0){
                    reasonCode=ReasonCode.SUCCESS;
                    properties=Collections.emptyList();
                }else {
                    reasonCode = ReasonCode.valueOf(buf.readByte(), fixedHeader.getPacketType());
                    if (readableBytes-buf.readableBytes()-fixedHeader.getRemainingLength()!=0) {
                        properties = decodeProperties(fixedHeader.getPacketType(), buf);
                    }else{
                        properties=Collections.emptyList();
                    }
                }
                break;

        }

        return new MqttPapaPacket(fixedHeader,packetIdentifier,reasonCode,properties);
    }
    /**
     * decode properties
     * @param controlPacketType
     * @param buf
     * @return
     */
    private List<MqttProperties> decodeProperties(ControlPacketType controlPacketType,ByteBuf buf){
        if (version==MqttVersion.MQTT_3_1_1){
            return null;
        }
        int propertiesLength = decodeVariableLength(buf);
        if (propertiesLength==0)
            return Collections.emptyList();
        List<MqttProperties> list=new ArrayList<>();
        ByteBuf slice = slice(buf, propertiesLength);
        while (slice.isReadable()){
            byte b = slice.readByte();
            MqttProperty property = MqttProperty.valueOf(b,controlPacketType);
            MqttProperties properties=null;
            if (property.isByte()){
                byte v = slice.readByte();

                properties=new MqttProperties(property,v);
            }else if (property.isBinaryData()){
                ByteBuf data = readBinaryData(slice);

                properties=new MqttProperties(property,data);
            }else if (property.isFourByteInteger()){
                long v = slice.readUnsignedInt();

                properties=new MqttProperties(property,v);
            }else if (property.isString()){
                String value=readUtf8String(slice);

                properties=new MqttProperties(property,value);
            }else if (property.isStringPair()){
                String key = readUtf8String(slice);
                String value = readUtf8String(slice);

                properties=new MqttProperties(property,new StringPair(key,value));

            }else if (property.isTwoByteInteger()){
                int v = slice.readUnsignedShort();

                properties=new MqttProperties(property,v);
            }else if (property.isVariableByteInteger()){

                properties=new MqttProperties(property,decodeVariableLength(slice));
            }
            list.add(properties);
        }

        Set<MqttProperty> propertySet=new HashSet<>();
        for (MqttProperties mqttProperties : list) {
            if (mqttProperties.getProperty()!= USER_PROPERTY&&mqttProperties.getProperty()!= SUBSCRIPTION_IDENTIFIER) {
                if (propertySet.contains(mqttProperties.getProperty()))
                    throw new ProtocolErrorException("multiple "+mqttProperties.getProperty());
                propertySet.add(mqttProperties.getProperty());
            }
        }
        return list;
    }

    private int decodeVariableLength(ByteBuf buf){
        int length=0;
        int multiplier = 1;
        short encodedByte;
        do {
            encodedByte = buf.readUnsignedByte();
            length += (encodedByte & 127) *multiplier;
            if (multiplier>128*128*128){
                throw new MalformedPacketException("Malformed Variable Byte Integer");
            }
            multiplier *= 128;
        }while ((encodedByte & 128) != 0&&buf.isReadable());
        if ((encodedByte & 128) != 0&&!buf.isReadable())
            return -1;
        return length;
    }

    private String readUtf8String(ByteBuf buf){
        int len = buf.readUnsignedShort();
        return buf.readSlice(len).toString(CharsetUtil.UTF_8);
    }

    private ByteBuf readBinaryData(ByteBuf buf){
        int length = buf.readUnsignedShort();
        return Unpooled.copiedBuffer(buf.readSlice(length));
    }

    private ByteBuf slice(ByteBuf buf,int length){
        return buf.readSlice(length);
    }

}
