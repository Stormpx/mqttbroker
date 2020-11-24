package com.stormpx.server;

import com.stormpx.ex.PacketTooLagerException;
import com.stormpx.ex.ProtocolErrorException;
import com.stormpx.kit.StringPair;
import com.stormpx.message.*;
import com.stormpx.mqtt.*;
import com.stormpx.mqtt.packet.*;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.function.Function;


public abstract class AbstractMqttContext implements MqttContext {
    protected final static Logger logger= LoggerFactory.getLogger("mqttbroker");
    protected MqttSocket mqttSocket;
    protected MqttSessionOption mqttSessionOption;
    protected MqttSessionImpl mqttSession;
    protected Handler<Throwable> exceptionHandler;

    protected Handler<MqttPublishMessage> publishHandler;
    protected Handler<Integer> publishAcknowledgeHandler;
    protected Function<Integer,Future<Void>> publishReceiveHandler;
    protected Handler<Integer> publishReleaseHandler;
    protected Handler<Integer> publishCompleteHandler;
    protected Handler<MqttSubscribeMessage> subscribeHandler;
    protected Handler<MqttUnSubscribeMessage> unSubscribeHandler;
    private Handler<MqttDisconnectMessage> disconnectHandler;
    private Handler<Void> closeHandler;


    private String id;
    protected boolean accept;
    protected boolean connectAck;
    protected boolean takenOver;
    protected MqttVersion version;
    protected int keepAlive;
    protected List<MqttProperties> properties;
    protected List<StringPair> userProperty;
    protected boolean cleanSession;
    protected boolean close;
    protected boolean disconnect;
    protected boolean isRequestResponseInformation;

    protected List<MqttProperties> ackProperties;

    protected Map<Integer, MqttInFlightMessage> inFlightMap;
    private Deque<MqttInFlightMessage> writeQueue;


    public AbstractMqttContext(MqttSocket mqttSocket, MqttConnectPacket connectPacket) {
        this.mqttSocket=mqttSocket;
        this.mqttSessionOption = mqttSocket.getSessionOption();
        this.version = connectPacket.getVersion();
        this.keepAlive = connectPacket.getKeepAlive();
        this.properties = connectPacket.getProperties();
        this.cleanSession = connectPacket.isCleanStart();
        this.isRequestResponseInformation =false;

        this.writeQueue=new LinkedList<>();
        this.inFlightMap=new HashMap<>();

        init(connectPacket);

        mqttSocket.pause();
    }

    private void init(MqttConnectPacket connectPacket){
        mqttSocket.closeHandler(v->{
            close=true;
            if (closeHandler!=null){
                closeHandler.handle(v);
            }
        });

        String authMethod = null;
        Buffer authData = null;
        this.userProperty=new ArrayList<>();
        if (connectPacket.getProperties()!=null) {
            for (MqttProperties properties : connectPacket.getProperties()) {
                switch (properties.getProperty()) {
                    case SESSION_EXPIRY_INTERVAL:
                        mqttSessionOption.setSessionExpiryInterval((Long) properties.getValue());
                        break;
                    case RECEIVE_MAXIMUM:
                        mqttSessionOption.setEndPointReceiveMaximum((Integer) properties.getValue());
                        break;
                    case MAXIMUM_PACKET_SIZE:
                        mqttSessionOption.setEndPointMaximumPacketSize((Long) properties.getValue());
                        break;
                    case TOPIC_ALIAS_MAXIMUM:
                        mqttSessionOption.setEndPointTopicAliasMaximum((Integer) properties.getValue());
                        break;
                    case REQUEST_RESPONSE_INFORMATION:
                        isRequestResponseInformation = ((byte) properties.getValue() == 1);
                        break;
                    case REQUEST_PROBLEM_INFORMATION:
                        mqttSessionOption.setRequestProblemInformation(((byte) properties.getValue() == 1));
                        break;
                    case USER_PROPERTY:
                        this.userProperty.add((StringPair) properties.getValue());
                        break;
                    case AUTHENTICATION_METHOD:
                        authMethod = properties.getValue().toString();
                        break;
                    case AUTHENTICATION_DATA:
                        authData = Buffer.buffer((ByteBuf) properties.getValue());
                        break;
                }
            }
        }
        boolean willFlag = connectPacket.isWillFlag();
        MqttWill will=new MqttWill(willFlag,connectPacket.getWillProperties(),connectPacket.isWillRetain(),
                connectPacket.getWillQos(),connectPacket.getWillTopic(),willFlag?Buffer.buffer(connectPacket.getWillPayload()):Buffer.buffer());

        ByteBuf password = connectPacket.getPassword();
        MqttAuth auth = new MqttAuth(connectPacket.getUserName(),mqttSocket.remoteAddress(), password==null?Buffer.buffer():Buffer.buffer(password), authMethod,authData);

        this.mqttSession=new MqttSessionImpl(mqttSessionOption,connectPacket.getClientIdentifier(),will,auth);
        this.id=UUID.randomUUID().toString();
    }

    void handle(MqttPacket packet) {

        try {
            if (packet.invalid()){
                throw packet.cause();
            }
            if (packet.fixedHeader().getRemainingLength()>mqttSessionOption.getMaximumPacketSize()){
                throw new PacketTooLagerException();
            }
            switch (packet.fixedHeader().getPacketType()){
                case PUBLISH:
                    handlePublish((MqttPublishPacket) packet);
                    break;
                case PUBACK:
                    handlePublishAcknowledge((MqttPapaPacket) packet);
                    break;
                case PUBREC:
                    handlePublishReceive((MqttPapaPacket) packet);
                    break;
                case PUBCOMP:
                    handlePublishComplete((MqttPapaPacket) packet);
                    break;
                case PUBREL:
                    handlePublishRelease((MqttPapaPacket) packet);
                    break;
                case SUBSCRIBE:
                    handleSubscribe((MqttSubscribePacket) packet);
                    break;
                case UNSUBSCRIBE:
                    handleUnSubscribe((MqttUnSubscribePacket) packet);
                    break;
                case PINGREQ:
                    handlePingReq();
                    break;
                case DISCONNECT:
                    handleDisconnect((MqttPapaPacket) packet);
                    break;
                case AUTH:
                    handleAuth((MqttPapaPacket) packet);
                    break;
            }
        } catch (Throwable e) {
            handleException(e);
        }

    }

    protected void handlePublish(MqttPublishPacket publishPacket){
        MqttPublishMessage mqttPublishMessage = getPublishMessage(publishPacket);
        MqttQoS qos = mqttPublishMessage.getQos();
        if (qos!=MqttQoS.AT_MOST_ONCE){
            int packetId = mqttPublishMessage.getPacketId();
            if (mqttSession.containsPacketId(packetId)){
                if (qos == MqttQoS.AT_LEAST_ONCE) {
                    //PACKET_IDENTIFIER_IN_USE
                    publishAcknowledge(packetId, ReasonCode.PACKET_IDENTIFIER_IN_USE, null, "packet identifier "+packetId+ " in use");
                } else {
                    //PUBREC
                    publishReceived(packetId, ReasonCode.SUCCESS, null, null);
                }
                return;
            }
        }

        if (qos!=MqttQoS.AT_MOST_ONCE&&mqttSession.endPointUsedQuota()>=mqttSessionOption.getReceiveMaximum()){
            // quota exceeded
            if (qos==MqttQoS.AT_LEAST_ONCE){
                publishAcknowledge(mqttPublishMessage.getPacketId(),ReasonCode.QUOTA_EXCEEDED,null,"quota exceeded");
            }else{
                publishReceived(mqttPublishMessage.getPacketId(),ReasonCode.QUOTA_EXCEEDED,null,"quota exceeded");
            }
            return;
        }

        if (qos==MqttQoS.EXACTLY_ONCE){
            mqttSession.addPacketId(mqttPublishMessage.getPacketId());
        }

        Handler<MqttPublishMessage> publishHandler = this.publishHandler;
        if (publishHandler !=null) {
            publishHandler.handle(mqttPublishMessage);
        }

    }

    protected abstract MqttPublishMessage getPublishMessage(MqttPublishPacket publishPacket);

    public  void handleException(Throwable throwable){
//        logger.info("client:"+mqttSession.clientIdentifier()+" throw ex cause:{}",throwable);
        Handler<Throwable> exceptionHandler = this.exceptionHandler;
        if (exceptionHandler!=null){
            exceptionHandler.handle(throwable);
        }
    }

    protected void handlePublishAcknowledge(MqttPapaPacket pubAckPacket){
        inFlightMap.remove(pubAckPacket.getPacketIdentifier());
        publishAcknowledge(pubAckPacket.getPacketIdentifier());
        writeNext();
    }

    protected void handlePublishReceive(MqttPapaPacket pubRecPacket){
        //if reason code != success complete publish
        if (pubRecPacket.getReasonCode()!= ReasonCode.SUCCESS){
            inFlightMap.remove(pubRecPacket.getPacketIdentifier());
            publishComplete(pubRecPacket.getPacketIdentifier());
            writeNext();
        }else {

            //call receive handler
            publishReceive(pubRecPacket.getPacketIdentifier())
                    .onFailure(this::handleException)
                    .onSuccess(v->{
                        MqttInFlightMessage mqttInFlightMessage = inFlightMap.get(pubRecPacket.getPacketIdentifier());
                        if (mqttInFlightMessage != null) {
                            mqttInFlightMessage.setReceived(true);
                        }
                        publishRelease( pubRecPacket.getPacketIdentifier(),ReasonCode.SUCCESS,null,null);
                    });
        }
    }


    protected void handlePublishComplete(MqttPapaPacket pubCompPacket){
        inFlightMap.remove(pubCompPacket.getPacketIdentifier());
        publishComplete(pubCompPacket.getPacketIdentifier());
        writeNext();
    }

    protected void handlePublishRelease(MqttPapaPacket pubRelPacket){
        int packetId = pubRelPacket.getPacketIdentifier();
        if (!mqttSession.containsPacketId(packetId)){
            publishComplete(packetId,ReasonCode.PACKET_IDENTIFIER_NOT_FOUND,null,"packet identifier not found");
        }
        publishRelease(packetId);
    }

    protected abstract void handleSubscribe(MqttSubscribePacket subscribePacket);

    protected abstract void handleUnSubscribe(MqttUnSubscribePacket unSubscribePacket);

    protected void handlePingReq(){
        if (isClose())
            return;
//        logger.debug("clientId :{} ping req ",mqttSession.clientIdentifier());
        MqttPapaPacket pingResp=new MqttPapaPacket(new FixedHeader(ControlPacketType.PINGRESP,false,0,false,0),
                0,null,null);

        mqttSocket.writePacket(pingResp);

    }

    protected void handleDisconnect(MqttPapaPacket packet){
        String reasonString=null;
        List<StringPair> userProperty=new ArrayList<>();
        Long sessionExpiryInterval=null;
        for (MqttProperties mqttProperties : packet.getProperties()) {
            switch (mqttProperties.getProperty()){
                case REASON_STRING:
                    reasonString= (String) mqttProperties.getValue();
                    break;
                case USER_PROPERTY:
                    userProperty.add((StringPair) mqttProperties.getValue());
                    break;
                case SESSION_EXPIRY_INTERVAL:
                    sessionExpiryInterval= (Long) mqttProperties.getValue();
                    if (mqttSessionOption.getSessionExpiryInterval()==0&&sessionExpiryInterval!=0)
                        throw new ProtocolErrorException("connect packet session expiry interval was zero but disconnect packet session expiry interval is a non-zero");
                    break;
            }
        }
        if (sessionExpiryInterval!=null)
            mqttSessionOption.setSessionExpiryInterval(sessionExpiryInterval);

        if (packet.getReasonCode()==ReasonCode.SUCCESS) {
            MqttDisconnectMessage disconnectMessage = new MqttDisconnectMessage(userProperty, reasonString);
            disconnect = true;
            if (disconnectHandler != null) {
                disconnectHandler.handle(disconnectMessage);
            }
        }else{
           /* if (packet.getReasonCode()==ReasonCode.DISCONNECT_WITH_WILL_MESSAGE)
                mqttSessionOption.setSessionExpiryInterval(0);*/

            logger.debug("client:{} receive disconnect reason code:{}",mqttSession.clientIdentifier(),packet.getReasonCode().byteValue());
        }
        close();

    }

    protected void handleAuth(MqttPapaPacket packet){
        //todo
        throw new ProtocolErrorException();
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public MqttVersion version() {
        return version;
    }

    @Override
    public boolean isCleanSession() {
        return cleanSession;
    }

    @Override
    public boolean isRequestResponseInformation() {
        return isRequestResponseInformation;
    }

    @Override
    public boolean isClose() {
        return close;
    }

    @Override
    public boolean isDisConnect() {
        return disconnect;
    }

    @Override
    public boolean isTakenOver() {
        return takenOver;
    }

    @Override
    public long sessionExpiryInterval() {
        return mqttSessionOption.getSessionExpiryInterval();
    }


    @Override
    public int keepAlive() {
        return keepAlive;
    }

    @Override
    public int topicAliasMaximum() {
        return mqttSessionOption.getTopicAliasMaximum();
    }

    @Override
    public boolean isAccept() {
        return accept;
    }

    @Override
    public List<StringPair> userProperty() {
        return userProperty;
    }

    @Override
    public MqttSession session() {
        return mqttSession;
    }

    @Override
    public MqttContext setSessionExpiryInterval(long sessionExpiryInterval) {
        mqttSessionOption.setSessionExpiryInterval(sessionExpiryInterval);
        addProperties(new MqttProperties(MqttProperty.SESSION_EXPIRY_INTERVAL,sessionExpiryInterval));
        return this;
    }

    @Override
    public MqttContext setKeepAlive(int keepAlive) {
        this.keepAlive=keepAlive;
        addProperties(new MqttProperties(MqttProperty.SERVER_KEEP_ALIVE,keepAlive));
        return this;
    }

    @Override
    public MqttContext setReceiveMaximum(int max) {
        max=max<1||max>65535?65535:max;
        mqttSessionOption.setReceiveMaximum(max);
        addProperties(new MqttProperties(MqttProperty.RECEIVE_MAXIMUM, max));
        return this;
    }

    @Override
    public MqttContext setMaximumPacketSize(long max) {
        mqttSessionOption.setMaximumPacketSize(max);
        return this;
    }

    @Override
    public MqttContext setClientIdentifier(String clientId) {
        addProperties(new MqttProperties(MqttProperty.ASSIGNED_CLIENT_IDENTIFIER,clientId));
        mqttSession.setClientIdentifier(clientId);
        return this;
    }

    @Override
    public MqttContext setRetainAvailable(boolean retainAvailable) {
        mqttSessionOption.setRetainAvailable(retainAvailable);
        addProperties(new MqttProperties(MqttProperty.RETAIN_AVAILABLE,(byte)(retainAvailable?1:0)));
        return this;
    }

    @Override
    public MqttContext setMaxQos(MqttQoS qos) {
        if (qos.value()<2) {
            addProperties(new MqttProperties(MqttProperty.MAXIMUM_QOS, (byte) qos.value()));
            mqttSessionOption.setMaxQos(qos);
        }
        return this;
    }

    @Override
    public MqttContext setWildcardSubscriptionAvailable(boolean subscriptionIdentifierAvailable) {
        addProperties(new MqttProperties(MqttProperty.WILDCARD_SUBSCRIPTION_AVAILABLE,(byte)(subscriptionIdentifierAvailable?1:0)));
        return this;
    }

    @Override
    public MqttContext setSubscriptionIdentifierAvailable(boolean subscriptionIdentifierAvailable) {
        addProperties(new MqttProperties(MqttProperty.SUBSCRIPTION_IDENTIFIER_AVAILABLE,(byte)(subscriptionIdentifierAvailable?1:0)));
        return this;
    }

    @Override
    public MqttContext setSharedSubscriptionAvailable(boolean sharedSubscriptionAvailable) {
        addProperties(new MqttProperties(MqttProperty.SHARED_SUBSCRIPTION_AVAILABLE,(byte)(sharedSubscriptionAvailable?1:0)));
        return this;
    }

    @Override
    public MqttContext setTopicAliasMaximum(int topicAliasMaximum) {
        if (topicAliasMaximum<0)
            topicAliasMaximum=0;
        if (topicAliasMaximum>65535)
            topicAliasMaximum=65535;
        mqttSessionOption.setTopicAliasMaximum(topicAliasMaximum);
        addProperties(new MqttProperties(MqttProperty.TOPIC_ALIAS_MAXIMUM,topicAliasMaximum));
        return this;
    }
    @Override
    public MqttContext addUserProperty(StringPair stringPair){
        addProperties(new MqttProperties(MqttProperty.USER_PROPERTY,stringPair));
        return this;
    }

    @Override
    public MqttContext addUserProperty(String key, String value) {
        return addUserProperty(new StringPair(key,value));
    }

    @Override
    public MqttContext setResponseInformation(String respInfo) {
        if (isRequestResponseInformation()) {
            addProperties(new MqttProperties(MqttProperty.RESPONSE_INFORMATION, respInfo));
        }
        return this;
    }


    @Override
    public MqttContext setAuthMethod(String authMethod) {
        addProperties(new MqttProperties(MqttProperty.AUTHENTICATION_METHOD,authMethod));
        return this;
    }

    @Override
    public MqttContext setAuthData(Buffer data) {
        addProperties(new MqttProperties(MqttProperty.AUTHENTICATION_DATA,data.getByteBuf()));
        return this;
    }

    @Override
    public MqttContext accept(boolean sessionPresent) {
        accept0(sessionPresent);
        //set channel
        connectAck=true;
        accept=true;
        mqttSocket.setKeepAlive(keepAlive);
        mqttSocket.resume();
        return this;
    }

    @Override
    public MqttContext takenOver(boolean sessionEnd) {
        if (!isClose()) {
            close();
            takenOver=true;
            if (sessionEnd)
                mqttSessionOption.setSessionExpiryInterval(0);
        }
        return this;
    }


    @Override
    public void publishRelease(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString) {
        if (!inFlightMap.containsKey(packetId)) {
            inFlightMap.put(packetId, MqttInFlightMessage.wrapper(null).setReceived(true));
            this.mqttSession.send(packetId);
        }
    }

    @Override
    public MqttContext exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler=handler;
        return this;
    }

    @Override
    public MqttContext publishHandler(Handler<MqttPublishMessage> handler) {
        this.publishHandler =handler;
        return this;
    }

    @Override
    public MqttContext publishAcknowledgeHandler(Handler<Integer> handler) {
        this.publishAcknowledgeHandler =handler;
        return this;
    }

    @Override
    public MqttContext publishReceiveHandler(Function<Integer,Future<Void>> handler) {
        this.publishReceiveHandler =handler;
        return this;
    }

    @Override
    public MqttContext publishReleaseHandler(Handler<Integer> handler) {
        this.publishReleaseHandler =handler;
        return this;
    }

    @Override
    public MqttContext publishCompleteHandler(Handler<Integer> handler) {
        this.publishCompleteHandler =handler;
        return this;
    }

    @Override
    public MqttContext subscribeHandler(Handler<MqttSubscribeMessage> handler) {
        this.subscribeHandler=handler;
        return this;
    }

    @Override
    public MqttContext unSubscribeHandler(Handler<MqttUnSubscribeMessage> handler) {
        this.unSubscribeHandler=handler;
        return this;
    }

    @Override
    public MqttContext disconnectHandler(Handler<MqttDisconnectMessage> handler) {
        this.disconnectHandler=handler;
        return this;
    }

    @Override
    public MqttContext closeHandler(Handler<Void> handler) {
        this.closeHandler=handler;
        return this;
    }

    protected abstract void accept0(boolean sessionPresent);




    protected void close(){
        if (!close) {
            close = true;
            mqttSocket.close();
        }
    }

    /**
     * release packetId and call handler
     * @param messageId packetId
     */
    private void publishAcknowledge(int messageId){
        this.mqttSession.release(messageId);
        if (publishAcknowledgeHandler !=null){
            publishAcknowledgeHandler.handle(messageId);
        }
    }

    /**
     *  call handler
     * @param messageId packetId
     */
    private Future<Void> publishReceive(int messageId){
        if (publishReceiveHandler !=null){
            return publishReceiveHandler.apply(messageId);
        }
        return Future.succeededFuture();
    }
    /**
     *  call handler
     * @param messageId packetId
     */
    private void publishRelease(int messageId){
        if (publishReleaseHandler !=null){
            publishReleaseHandler.handle(messageId);
        }

    }

    /**
     * release packetId and call handler
     * @param messageId packetId
     */
    private void publishComplete(int messageId){
        this.mqttSession.release(messageId);
        if (publishCompleteHandler !=null){
            publishCompleteHandler.handle(messageId);
        }
    }

    @Override
    public Future<Integer> publish(MqttBrokerMessage message) {
        MqttInFlightMessage inFlightMessage = MqttInFlightMessage.wrapper(message);
        writeQueue.addLast(inFlightMessage);
        writeNext();

        return inFlightMessage.getPromise().future();
    }

    /**
     * write by queue
     */
    private void writeNext(){
        if (writeQueue.isEmpty()||isClose())
            return;
        if (inFlightMap.size()>= mqttSessionOption.getEndPointReceiveMaximum())
            return;

        MqttInFlightMessage inFlightMessage = writeQueue.poll();
        MqttBrokerMessage message = inFlightMessage.getMessage();
        if (message==null){
            //should not be null
            writeNext();
            return;
        }

        if (message.getQos()==MqttQoS.AT_MOST_ONCE){
            writeMessage(message).setHandler(ar->{
                inFlightMessage.getPromise().complete();
                writeNext();
            });
        }else{
            // wrapper message
            Integer packetId = message.getPacketId();
            if (packetId==null||packetId==0){
                packetId=this.mqttSession.nextMessageId();
                message.setPacketId(packetId);
            }
            writeMessage(message).setHandler(ar->{
                if (ar.succeeded()){
                    inFlightMap.put(message.getPacketId(), inFlightMessage);
                    this.mqttSession.send(message.getPacketId());
                    inFlightMessage.getPromise().complete(message.getPacketId());
                }else{
                    if (ar.cause() instanceof PacketTooLagerException){
                        //ignore
                       /* if (message.getQos()==MqttQoS.EXACTLY_ONCE){
                            publishComplete(message.getPacketId());
                        }else {
                            publishAcknowledge(message.getPacketId());
                        }*/
                    }else{
                        handleException(ar.cause());
                    }
                    inFlightMessage.getPromise().fail(ar.cause());
                }
            });
        }

    }

    /**
     * actually write operation
     * @param message
     * @return
     */
    protected abstract Future<Void> writeMessage(MqttBrokerMessage message);


    private void addProperties(MqttProperties properties){
        if (version==MqttVersion.MQTT_3_1_1)
            return;
        if (!properties.getProperty().useful(ControlPacketType.CONNACK))
            throw new RuntimeException();
        if (ackProperties==null)
            ackProperties=new ArrayList<>();
        if (properties.getProperty()==MqttProperty.USER_PROPERTY){
            ackProperties.add(properties);
            return;
        }
        for (int i = 0; i < ackProperties.size(); i++) {
            if (ackProperties.get(0).getProperty()==properties.getProperty()) {
                ackProperties.remove(i);
                break;
            }
        }
        ackProperties.add(properties);
    }
}
