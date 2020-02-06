package com.stormpx.server;

import com.stormpx.message.*;
import com.stormpx.ex.*;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.*;
import com.stormpx.mqtt.packet.*;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;

import java.time.Instant;
import java.util.*;

public class Mqtt5Context extends AbstractMqttContext {

    private int alias=1;
    private Map<String,Integer> topicToAliasMap;
    private Map<Integer,String> aliasToTopicMap;

    public Mqtt5Context(MqttSocket mqttSocket, MqttConnectPacket connectPacket) {
        super(mqttSocket,connectPacket);
        this.topicToAliasMap=new HashMap<>();
        this.aliasToTopicMap=new HashMap<>();
    }

    @Override
    protected void handlePublish(MqttPublishPacket publishPacket) {
        FixedHeader fixedHeader = publishPacket.fixedHeader();
        if (fixedHeader.isRetain()&&!mqttSessionOption.isRetainAvailable()) {
            throw new RetainNotSupportedException("retain not support");
        }
        if (fixedHeader.getQos()>mqttSessionOption.getMaxQos().value())
            throw new QosNotSupportedException("publish qos "+fixedHeader.getQos()+" > maxQos "+mqttSessionOption.getMaxQos()+" not support");

        MqttPublishMessage mqttPublishMessage =
                new MqttPublishMessage(publishPacket.getTopicName(), fixedHeader.getQosAsEnum(), fixedHeader.isRetain(), fixedHeader.isDup(),
                        Buffer.buffer(publishPacket.getPayload()), publishPacket.getPacketIdentifier());


        for (Iterator<MqttProperties> iterator = publishPacket.getProperties().iterator(); iterator.hasNext(); ) {
            MqttProperties properties = iterator.next();
            switch (properties.getProperty()) {
                case PAYLOAD_FORMAT_INDICATOR:
                    mqttPublishMessage.setPayloadFormatIndicator((byte) properties.getValue() == 1);
                    break;
                case RESPONSE_TOPIC:
                    mqttPublishMessage.setResponseTopic((String) properties.getValue());
                    break;
                case CORRELATION_DATA:
                    mqttPublishMessage.setCorrelationData(Buffer.buffer((ByteBuf) properties.getValue()));
                    break;
                case USER_PROPERTY:
                    mqttPublishMessage.addUserProperty((StringPair) properties.getValue());
                    break;
                case CONTENT_TYPE:
                    mqttPublishMessage.setContentType((String) properties.getValue());
                    break;
                case SUBSCRIPTION_IDENTIFIER:
                    throw new ProtocolErrorException("client sent to server must not contain a subscription identifier");
                case MESSAGE_EXPIRY_INTERVAL:
                    Long propertiesValue = (Long) properties.getValue();
                    mqttPublishMessage.setMessageExpiryInterval(propertiesValue);
                    break;
                case TOPIC_ALIAS:
                    Integer value = (Integer) properties.getValue();
                    if (value==0||value>mqttSessionOption.getTopicAliasMaximum()){
                        throw new InvalidTopicAliasException();
                    }
                    if (mqttPublishMessage.getTopic()==null||mqttPublishMessage.getTopic().isEmpty()){
                        String topic = aliasToTopicMap.get(value);
                        if (topic==null){
                            throw new ProtocolErrorException("does not already have a mapping for this topic alias");
                        }
                        mqttPublishMessage.setTopic(topic);
                    }else{
                        aliasToTopicMap.put(value,mqttPublishMessage.getTopic());
                    }
                    mqttPublishMessage.setTopicAlias((Integer) properties.getValue());
                    break;
            }
        }
        mqttPublishMessage.setExpiryTimestamp(Instant.now().getEpochSecond()+mqttPublishMessage.getMessageExpiryInterval());


        if (publishHandler!=null) {
            publishHandler.handle(mqttPublishMessage);
        }
    }

    @Override
    protected void handlePublishReceive(MqttPapaPacket pubRecPacket) {
        if (pubRecPacket.getReasonCode()== ReasonCode.SUCCESS) {
            MqttInFlightMessage flightMessage = inFlightMap.get(pubRecPacket.getPacketIdentifier());
            if (flightMessage == null) {
                publishRelease( pubRecPacket.getPacketIdentifier(),ReasonCode.PACKET_IDENTIFIER_NOT_FOUND,null,null);
            } else {
                publishRelease( pubRecPacket.getPacketIdentifier(),ReasonCode.SUCCESS,null,null);
            }
        }
        //call super
        super.handlePublishReceive(pubRecPacket);
    }

    @Override
    protected void handleSubscribe(MqttSubscribePacket subscribePacket) {
        int subId = 0;
        List<StringPair> list = new ArrayList<>();

        for (MqttProperties properties : subscribePacket.getProperties()) {
            switch (properties.getProperty()) {
                case SUBSCRIPTION_IDENTIFIER:
                    if ((Integer) properties.getValue() == 0) {
                        throw new ProtocolErrorException("subscription identifier is 0");
                    }
                    subId = (int) properties.getValue();
                    break;
                case USER_PROPERTY:
                    list.add((StringPair) properties.getValue());
                    break;
            }
        }

        MqttSubscribeMessage subscribeMessage = new MqttSubscribeMessage(subscribePacket.getSubscriptions(), subscribePacket.getPacketIdentifier(), subId, list);

        if (subscribeHandler != null) {
            subscribeHandler.handle(subscribeMessage);
        }
    }


    @Override
    protected void handleUnSubscribe(MqttUnSubscribePacket unSubscribePacket) {
        List<StringPair> list=new ArrayList<>();
        for (MqttProperties properties : unSubscribePacket.getProperties()) {
            switch (properties.getProperty()){
                case USER_PROPERTY:
                    list.add((StringPair) properties.getValue());
                    break;
            }
        }
        MqttUnSubscribeMessage mqttUnSubscribeMessage = new MqttUnSubscribeMessage(unSubscribePacket.getSubscriptions(), unSubscribePacket.getPacketIdentifier(), list);
        if (unSubscribeHandler!=null){
            unSubscribeHandler.handle(mqttUnSubscribeMessage);
        }
    }

    @Override
    public void handleException(Throwable throwable){
        //check exception
        super.handleException(throwable);
        if (isClose())
            return;
        ReasonCode reasonCode=null;
        if (throwable instanceof PacketTooLagerException){
            reasonCode=ReasonCode.PACKET_TOO_LARGE;
        }else if (throwable instanceof ProtocolErrorException){
            reasonCode=ReasonCode.PROTOCOL_ERROR;
        } else if (throwable instanceof MalformedPacketException){
            reasonCode=ReasonCode.MALFORMED_PACKET;
        }else if (throwable instanceof InvalidTopicAliasException){
            reasonCode=ReasonCode.TOPIC_ALIAS_INVALID;
        } else if (throwable instanceof RetainNotSupportedException){
            reasonCode=ReasonCode.RETAIN_NOT_SUPPORTED;
        }else if (throwable instanceof QosNotSupportedException){
            reasonCode=ReasonCode.QOS_NOT_SUPPORTED;
        }else if (throwable instanceof WildcardSubscriptionsNotSupportedException){
            reasonCode=ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED;
        }else if (throwable instanceof SubscriptionIdNotSupportedException){
            reasonCode=ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED;
        }else if (throwable instanceof SharedSubscriptionsNotSupportedException){
            reasonCode=ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED;
        }else{
            reasonCode=ReasonCode.UNSPECIFIED_ERROR;
        }
        List<MqttProperties> properties=null;
        properties=new LinkedList<>();
        if (mqttSessionOption.isRequestProblemInformation()&&throwable.getMessage()!=null&&!throwable.getMessage().isBlank()) {
            properties.add(new MqttProperties(MqttProperty.REASON_STRING,throwable.getMessage() ));
        }

        MqttPacket packet=null;
        //disconnect
        if (connectAck) {
            packet = new MqttPapaPacket(FixedHeader.DISCONNECT,
                    0, reasonCode, properties);
        }else {
            packet = new MqttConnAckPacket(FixedHeader.CONNACK,
                    reasonCode.byteValue(), properties, false);
        }
        mqttSocket.writePacket(packet,ar->{
            if (ar.failed()){
                if (exceptionHandler!=null){
                    exceptionHandler.handle(ar.cause());
                }else{
                    ar.cause().printStackTrace();
                }
            }
            close();
        });
    }
    @Override
    protected void accept0(boolean sessionPresent) {
        if (connectAck||close)
            return;
        MqttConnAckPacket packet=new MqttConnAckPacket(FixedHeader.CONNACK,
                ReasonCode.SUCCESS.byteValue(),ackProperties==null?Collections.emptyList():ackProperties,sessionPresent);
        mqttSocket.writePacket(packet);
    }

    @Override
    public MqttContext reject(byte reasonCode) {
        if (connectAck)
            return this;
        ReasonCode code = ReasonCode.valueOf(reasonCode);
        if (code.useful(ControlPacketType.CONNACK)){
            code=ReasonCode.IMPLEMENTATION_SPECIFIC_ERROR;
        }
        MqttConnAckPacket packet=new MqttConnAckPacket(FixedHeader.CONNACK,
                code.byteValue(),ackProperties==null?Collections.emptyList():ackProperties,false);
        mqttSocket.writePacket(packet,ar->{
           close();
        });
        logger.debug("reject client :{} by code:{}",mqttSession.clientIdentifier(),code);
        return this;
    }

    @Override
    protected Future<Void> writeMessage(MqttBrokerMessage message) {
        List<MqttProperties> list=new LinkedList<>(message.getProperties());
        ByteBuf payload= Unpooled.copiedBuffer(message.getPayload().getByteBuf());

        if (message.getMessageExpiryInterval()!=null){
            list.add(new MqttProperties(MqttProperty.MESSAGE_EXPIRY_INTERVAL,message.getMessageExpiryInterval()));
        }

        //topic alias
        Integer alias = topicToAliasMap.get(message.getTopic());
        if (alias!=null){
            list.add(new MqttProperties(MqttProperty.TOPIC_ALIAS,alias));
        } else if (topicToAliasMap.size()<mqttSessionOption.getEndPointTopicAliasMaximum()){
            int a = this.alias;
            this.alias+=1;
            list.add(new MqttProperties(MqttProperty.TOPIC_ALIAS,a));
        }

        /*if (message.getTopicAlias()!=0){
            list.add(new MqttProperties(MqttProperty.TOPIC_ALIAS,message.getTopicAlias()));
        }*/

        if (message.getSubscriptionId()!=null){
            message.getSubscriptionId()
                    .stream()
                    .map(id->new MqttProperties(MqttProperty.SUBSCRIPTION_IDENTIFIER,id))
                    .forEach(list::add);
        }

        MqttPublishPacket publishPacket=new MqttPublishPacket(new FixedHeader(ControlPacketType.PUBLISH,message.isDup(),message.getQos().value(),message.isRetain(),0),
                message.getTopic(),message.getPacketId(),list,payload);

        Promise<Void> promise=Promise.promise();
        mqttSocket.writePacket(publishPacket,promise);
        return promise.future();
    }

    @Override
    public MqttContext reject(boolean permanent, String serverReference) {
        if (connectAck)
            return this;
        List<MqttProperties> properties= Arrays.asList(new MqttProperties(MqttProperty.SERVER_REFERENCE,serverReference));
        MqttConnAckPacket packet=new MqttConnAckPacket(FixedHeader.CONNACK,
                (permanent?ReasonCode.SERVER_MOVED:ReasonCode.USE_ANOTHER_SERVER).byteValue(),properties,false);
        mqttSocket.writePacket(packet,ar->{
            close();
        });
        return this;
    }

    @Override
    public MqttContext takenOver(boolean sessionEnd) {
        if (!isClose()) {
            MqttPacket packet = new MqttPapaPacket(FixedHeader.DISCONNECT,
                    0, ReasonCode.SESSION_TAKEN_OVER, properties);
            mqttSocket.writePacket(packet);
        }
        return super.takenOver(sessionEnd);
    }

    @Override
    public void publishAcknowledge(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        List<MqttProperties> list = buildProperties(userProperty, reasonString);

        var packet=new MqttPapaPacket(FixedHeader.PUBACK,
                packetId,reasonCode,list);
        mqttSocket.writePacket(packet);
    }

    @Override
    public void publishReceived(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        List<MqttProperties> list = buildProperties(userProperty, reasonString);

        var packet=new MqttPapaPacket(FixedHeader.PUBREC,
                packetId,reasonCode,list);
        mqttSocket.writePacket(packet);
    }

    @Override
    public void publishRelease(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        List<MqttProperties> list = buildProperties(userProperty, reasonString);
        var packet=new MqttPapaPacket(FixedHeader.PUBREL,
                packetId,reasonCode,list);
        mqttSocket.writePacket(packet);
        if (ReasonCode.SUCCESS==reasonCode) {
            super.publishRelease(packetId, reasonCode, userProperty, reasonString);
        }
    }


    @Override
    public void publishComplete(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        List<MqttProperties> list = buildProperties(userProperty, reasonString);

        var packet=new MqttPapaPacket(FixedHeader.PUBCOMP,
                packetId,reasonCode,list);
        mqttSocket.writePacket(packet);
    }


    /**
     * response subscribeAcknowledge
     * @param packetId
     * @param reasonCodes
     * @param userProperty
     * @param reasonString
     */
    @Override
    public void subscribeAcknowledge(int packetId, List<ReasonCode> reasonCodes, List<StringPair> userProperty,String reasonString) {
        if (isClose())
            return;
        MqttUnsubOrSubAckPacket subAckPacket= buildUnsubOrSubPacket(ControlPacketType.SUBACK,packetId,reasonCodes,userProperty,reasonString);

        mqttSocket.writePacket(subAckPacket);

    }

    /**
     * response unSubscribeAcknowledge
     * @param packetId
     * @param reasonCodes
     * @param userProperty
     * @param reasonString
     */
    @Override
    public void unSubscribeAcknowledge(int packetId, List<ReasonCode> reasonCodes, List<StringPair> userProperty, String reasonString) {
        if (isClose())
            return;
        MqttUnsubOrSubAckPacket subAckPacket= buildUnsubOrSubPacket(ControlPacketType.UNSUBACK,packetId,reasonCodes,userProperty,reasonString);
        mqttSocket.writePacket(subAckPacket);
    }

    /**
     * buildUnsubOrSubPacket
     * @param controlPacketType
     * @param packetId
     * @param reasonCodes
     * @param userProperty
     * @param reasonString
     * @return
     */
    private MqttUnsubOrSubAckPacket buildUnsubOrSubPacket(ControlPacketType controlPacketType,int packetId, List<ReasonCode> reasonCodes, List<StringPair> userProperty,String reasonString){
        for (ReasonCode reasonCode : reasonCodes) {
            if (!reasonCode.useful(controlPacketType))
                throw new ProtocolErrorException("");
        }
        List<MqttProperties> properties = buildProperties(userProperty, reasonString);
        MqttUnsubOrSubAckPacket packet= new MqttUnsubOrSubAckPacket(new FixedHeader(controlPacketType,false,0,false,0),
                packetId,properties,reasonCodes);

        return packet;
    }

    /**
     *
     * @param userProperty
     * @param reasonString
     * @return
     */
    private List<MqttProperties> buildProperties(List<StringPair> userProperty, String reasonString) {
        List<MqttProperties> list=new ArrayList<>();
        if (userProperty!=null){
            userProperty.forEach(sp->list.add(new MqttProperties(MqttProperty.USER_PROPERTY,sp)));
        }
        if (mqttSessionOption.isRequestProblemInformation()&&reasonString!=null){
            list.add(new MqttProperties(MqttProperty.REASON_STRING,reasonString));
        }
        return list;
    }


}
