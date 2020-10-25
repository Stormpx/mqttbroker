package com.stormpx.broker;

import com.stormpx.Constants;
import com.stormpx.auth.AuthResult;
import com.stormpx.auth.Authenticator;
import com.stormpx.dispatcher.api.Session;
import com.stormpx.dispatcher.command.CloseSessionCommand;
import com.stormpx.dispatcher.command.PacketIdActionCommand;
import com.stormpx.ex.SharedSubscriptionsNotSupportedException;
import com.stormpx.ex.SubscriptionIdNotSupportedException;
import com.stormpx.ex.WildcardSubscriptionsNotSupportedException;
import com.stormpx.dispatcher.*;
import com.stormpx.kit.StringPair;
import com.stormpx.kit.TopicUtil;
import com.stormpx.message.*;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.MqttVersion;
import com.stormpx.mqtt.ReasonCode;
import com.stormpx.server.MqttContext;
import com.stormpx.server.MqttSession;
import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.store.MessageLink;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.stormpx.Constants.MQTT_MAX_MESSAGE_EXPIRY_INTERVAL;

public class ClientController {
    private final static Logger logger= LoggerFactory.getLogger(ClientController.class);

    private Vertx vertx;

    private BrokerController brokerController;

    private MqttContext mqttContext;


    private Authenticator authenticator;

    private EventController eventController;

    private Session session;


    public ClientController(Vertx vertx,MqttContext mqttContext,BrokerController brokerController,Authenticator authenticator,Session session) {
        this.vertx=vertx;
        this.mqttContext = mqttContext;
        this.brokerController=brokerController;
        this.authenticator=authenticator;
        this.session=session;
        tryAccept();
    }

    public MqttContext getMqttContext() {
        return mqttContext;
    }

    public String getAddress(){
        return eventController.getMessageConsumer().address();
    }


    public void send(DispatcherMessage dispatcherMessage, List<Integer> subscriptionIds){
        resend(null,dispatcherMessage,subscriptionIds);

    }

    public void resend(Integer packetId,DispatcherMessage dispatcherMessage, List<Integer> subscriptionIds){
        String id = dispatcherMessage.getId();
        String topic = dispatcherMessage.getTopic();
        MqttQoS qos = dispatcherMessage.getQos();
        boolean dup = dispatcherMessage.isDup();
        boolean retain = dispatcherMessage.isRetain();
        Buffer payload = dispatcherMessage.getPayload();

        if (mqttContext.isClose()){
            if (Instant.now().getEpochSecond()<=mqttContext.session().expiryTime()&&packetId==null&&qos!=MqttQoS.AT_MOST_ONCE){
                session.link(MessageLink.create(id, mqttContext.session().clientIdentifier(),null,retain,qos, subscriptionIds));
            }
            return;
        }

        MqttBrokerMessage mqttBrokerMessage = new MqttBrokerMessage().setTopic(topic)
                .setQos(qos).setRetain(retain).setDup(dup).setPayload(payload)
                .setMessageExpiryInterval(dispatcherMessage.getRemainTime())
                .setProperties(Optional.ofNullable(dispatcherMessage.getProperties()).orElse(Collections.emptyList()))
                .setSubscriptionId(subscriptionIds)
                ;

        if (packetId!=null){
            mqttBrokerMessage.setPacketId(packetId);
        }

        mqttContext.publish(mqttBrokerMessage)
                .setHandler(ar->{
                    if (ar.succeeded()){
                        Integer pid = ar.result();
                        if (packetId==null&&qos!=MqttQoS.AT_MOST_ONCE){
                            session.link(MessageLink.create(id, mqttContext.session().clientIdentifier(),pid,retain,qos, subscriptionIds));
                        }
                    }else{
                        logger.warn("send to message client: {} failed ",mqttContext.session().clientIdentifier());
                        logger.error(ar.cause());
                    }
                });
    }


    public void tryAccept(){
        logger.debug("client:{} try connect",mqttContext.session().clientIdentifier());
        mqttContext.exceptionHandler(t->{
            logger.error(mqttContext.session().clientIdentifier()+ " ex ",t);
        });
        if (!trySetOption(mqttContext))
            return;
        //auth
        if (mqttContext.session().clientIdentifier()==null)
            mqttContext.setClientIdentifier(UUID.randomUUID().toString());
        authenticator.authorize(mqttContext.session().clientIdentifier(),mqttContext.session().auth(),mqttContext.userProperty())
                .setHandler(ar->{
                    if (ar.succeeded()){
                        AuthResult<Boolean> authResult = ar.result();
                        List<StringPair> pairList = authResult.getPairList();
                        if (pairList!=null)
                            pairList.forEach(mqttContext::addUserProperty);
                        if (authResult.getObject()){
                            accept(mqttContext);
                        }else{
                            if (mqttContext.version()== MqttVersion.MQTT_3_1_1) {
                                mqttContext.reject(MqttConnectReturnCode.CONNECTION_REFUSED_BAD_USER_NAME_OR_PASSWORD.byteValue());
                            }else{
                                mqttContext.reject(ReasonCode.BAD_USER_NAME_OR_PASSWORD.byteValue());
                            }
                        }
                    }else{
                        logger.info("authorize client:{} fail ",mqttContext.session().clientIdentifier());
                        mqttContext.handleException(ar.cause());
                    }
                });
    }

    private boolean trySetOption(MqttContext mqttContext){
        JsonObject mqttConfig = brokerController.getMqttConfig();
        Boolean retainAvailable = mqttConfig.getBoolean(Constants.MQTT_RETAIN_AVAILABLE, true);
        if (!retainAvailable){
            mqttContext.setRetainAvailable(false);
            MqttWill will = mqttContext.session().will();
            if (will.isWillFlag()&&will.isRetain()){
                //rejcet
                if (mqttContext.version()==MqttVersion.MQTT_3_1_1){
                    mqttContext.reject(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED.byteValue());
                }else{
                    mqttContext.reject(ReasonCode.RETAIN_NOT_SUPPORTED.byteValue());
                }
                return false;
            }
        }

        Integer maxQos = mqttConfig.getInteger(Constants.MQTT_MAXIMUM_QOS);
        if (maxQos!=null) {
            if (maxQos > 2) maxQos = 2;
            if (maxQos < 0) maxQos = 0;
            mqttContext.setMaxQos(MqttQoS.valueOf(maxQos));
            MqttWill will = mqttContext.session().will();
            if (will.isWillFlag()&&will.getQos().value()>maxQos){
                //rejcet
                if (mqttContext.version()==MqttVersion.MQTT_3_1_1){
                    mqttContext.reject(MqttConnectReturnCode.CONNECTION_REFUSED_NOT_AUTHORIZED.byteValue());
                }else{
                    mqttContext.reject(ReasonCode.QOS_NOT_SUPPORTED.byteValue());
                }
                return false;
            }
        }

        Integer receiveMaximum = mqttConfig.getInteger(Constants.MQTT_RECEIVE_MAXIMUM);
        if (receiveMaximum!=null&&receiveMaximum>0&&receiveMaximum<=65535)
            mqttContext.setReceiveMaximum(receiveMaximum);

        Integer maxPacketSize = mqttConfig.getInteger(Constants.MQTT_MAXIMUM_PACKET_SIZE);
        if (maxPacketSize!=null&&maxPacketSize>0&&maxPacketSize<=268435455)
            mqttContext.setMaximumPacketSize(maxPacketSize);

        Integer topicAliasMaximum = mqttConfig.getInteger(Constants.MQTT_TOPIC_ALIAS_MAXIMUM);
        if (topicAliasMaximum!=null&&topicAliasMaximum>0&&topicAliasMaximum<=65535)
            mqttContext.setTopicAliasMaximum(topicAliasMaximum);

        Integer keepAlive = mqttConfig.getInteger(Constants.MQTT_SERVER_KEEP_ALIVE);
        if (keepAlive!=null&&keepAlive>0&&keepAlive<=65535)
            mqttContext.setKeepAlive(keepAlive);

        Boolean wildcard = mqttConfig.getBoolean(Constants.MQTT_WILDCARD_SUBSCRIPTION_AVAILABLE);
        if (wildcard!=null)
            mqttContext.setWildcardSubscriptionAvailable(wildcard);

        Boolean subscriptionIdentifierAvailable = mqttConfig.getBoolean(Constants.MQTT_SUBSCRIPTION_IDENTIFIER_AVAILABLE);
        if (subscriptionIdentifierAvailable!=null)
            mqttContext.setSubscriptionIdentifierAvailable(subscriptionIdentifierAvailable);

        Boolean sharedSubscriptionAvailable = mqttConfig.getBoolean(Constants.MQTT_SHARED_SUBSCRIPTION_AVAILABLE);
        if (sharedSubscriptionAvailable!=null)
            mqttContext.setSharedSubscriptionAvailable(sharedSubscriptionAvailable);

        Long maxSessionExpiryInterval = mqttConfig.getLong(Constants.MQTT_MAX_SESSION_EXPIRY_INTERVAL);
        if (maxSessionExpiryInterval!=null&&mqttContext.sessionExpiryInterval()>maxSessionExpiryInterval) {
            mqttContext.setSessionExpiryInterval(maxSessionExpiryInterval);
        }
        return true;
    }

    /**
     * accept connection
     * @param mqttContext
     */
    private void accept(MqttContext mqttContext){
        brokerController.tryAcceptSession(mqttContext.session().clientIdentifier(),mqttContext.isCleanSession(),mqttContext.id(),getAddress())
                .onFailure(t->{
                    logger.error(" try accept"+" client:"+mqttContext.session().clientIdentifier()+  " failed :{}",t.getMessage());
                    mqttContext.handleException(t);
                })
                .onSuccess(present->{
                    if (!present){
                        brokerController.clearSubscribe(mqttContext.session().clientIdentifier());
                    }
                    this.eventController=new EventController(vertx,mqttContext,brokerController,this);
                    mqttContext.subscribeHandler(this::onSubscribe)
                            .unSubscribeHandler(this::onUnSubscribe)
                            .publishHandler(this::onPublish)
                            .publishReleaseHandler(id ->{
                                session.packetId(PacketIdActionCommand.removePacketId(mqttContext.session().clientIdentifier(), id))
                                        .onFailure(mqttContext::handleException)
                                        .onSuccess(v->{
                                            mqttContext.publishComplete(id,ReasonCode.SUCCESS,null,null);
                                            mqttContext.session().removePacketId(id);
                                        });

                            })
                            .publishAcknowledgeHandler(id -> session.packetId(PacketIdActionCommand.release(mqttContext.session().clientIdentifier(), id)))
                            .publishReceiveHandler(id -> session.packetId(PacketIdActionCommand.discardMessage(mqttContext.session().clientIdentifier(), id)))
                            .publishCompleteHandler(id -> session.packetId(PacketIdActionCommand.release(mqttContext.session().clientIdentifier(), id)))
                            .disconnectHandler(message -> {
                                logger.info("client:{} disconnect userProperty:{}", mqttContext.session().clientIdentifier(), message.getUserProperty());
                            }).closeHandler(this::onClose);

                    mqttContext.accept(present);
                    logger.info("accept client:{}",mqttContext.session().clientIdentifier());
                    brokerController.add(this);
                });
    }

    /**
     * check option
     * @param message subscribe message
     */
    private void verifySubscribe(MqttSubscribeMessage message){
        JsonObject mqttConfig = brokerController.getMqttConfig();
        Boolean wildcard = mqttConfig.getBoolean(Constants.MQTT_WILDCARD_SUBSCRIPTION_AVAILABLE);
        Boolean subscriptionIdentifierAvailable = mqttConfig.getBoolean(Constants.MQTT_SUBSCRIPTION_IDENTIFIER_AVAILABLE);
        Boolean sharedSubscriptionAvailable = mqttConfig.getBoolean(Constants.MQTT_SHARED_SUBSCRIPTION_AVAILABLE);
        if (wildcard==null)
            wildcard=true;
        if (subscriptionIdentifierAvailable==null)
            subscriptionIdentifierAvailable=true;
        if (sharedSubscriptionAvailable==null)
            sharedSubscriptionAvailable=true;

        if (wildcard&&subscriptionIdentifierAvailable&&sharedSubscriptionAvailable)
            return ;

        if (!subscriptionIdentifierAvailable&&message.getSubscriptionIdentifier()!=0)
            throw new SubscriptionIdNotSupportedException("subscription identifier not supported");

        for (MqttSubscription mqttSubscription : message.getMqttSubscriptions()) {
            String topicFilter = mqttSubscription.getTopicFilter();
            if (!wildcard&& TopicUtil.containWildcard(topicFilter))
                throw new WildcardSubscriptionsNotSupportedException("topic: "+topicFilter+" wildcard not supported");
            if (!sharedSubscriptionAvailable&&TopicUtil.isShareTopicFilter(topicFilter))
                throw new SharedSubscriptionsNotSupportedException("topic: "+topicFilter+" share subscriptions not supported");
        }

    }
    /**
     * Subscribe handler
     * @param message
     */
    private void onSubscribe(MqttSubscribeMessage message){
        try {
            verifySubscribe(message);
            List<MqttSubscription> mqttSubscriptions = message.getMqttSubscriptions();
            //auth
            authenticator.authorizeSub(mqttContext.session().clientIdentifier(), mqttSubscriptions, message.getUserProperty())
                    .onFailure(t->logger.warn("client:{} authorize subscribe failed", mqttContext.session().clientIdentifier()))
                    .onFailure(mqttContext::handleException)
                    .compose(authResult->{
                        List<ReasonCode> reasonCodes = authResult.getObject();

                        Subscribe subscribe = new Subscribe(mqttContext.session().clientIdentifier(), mqttSubscriptions);

                        List<MqttSubscription> subscriptions = subscribe.filter(reasonCodes).getSubscriptions();

                        //sub
                        return brokerController.subscribe(mqttContext.session().clientIdentifier(),subscriptions, msg->{
                                    MqttSubscription matchSubscription = subscribe.getMaxMatchSubscription(msg.getMessage().getTopic());
                                    List<Integer> list=null;
                                    if (matchSubscription!=null) {
                                        int qos=Math.min(matchSubscription.getQos().value(), msg.getMessage().getQos().value());
                                        msg.getMessage().setQos(MqttQoS.valueOf(qos)).setRetain(true);
                                        if (message.getSubscriptionIdentifier()!=0){
                                            list=Collections.singletonList(message.getSubscriptionIdentifier());
                                        }
                                    }
                                    send(msg.getMessage(),list);
                                })
                                .onSuccess(v->mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), reasonCodes, authResult.getPairList(), null));

                    })
                    .onFailure(mqttContext::handleException);
        } catch (SubscriptionIdNotSupportedException e) {
            mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), message.getMqttSubscriptions().stream().map(s -> ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED).collect(Collectors.toList()), null, null);
        } catch (WildcardSubscriptionsNotSupportedException e) {
            mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), message.getMqttSubscriptions().stream().map(s -> ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED).collect(Collectors.toList()), null, null);
        } catch (SharedSubscriptionsNotSupportedException e) {
            mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), message.getMqttSubscriptions().stream().map(s -> ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED).collect(Collectors.toList()), null, null);
        }
    }


    /**
     * UnSubscribe handler
     * @param message
     */
    private void onUnSubscribe( MqttUnSubscribeMessage message){
        List<String> mqttSubscriptions = message.getMqttSubscriptions();
        String clientId = mqttContext.session().clientIdentifier();

        List<ReasonCode> reasonCodes = Stream.generate(()->ReasonCode.SUCCESS).limit(mqttSubscriptions.size()).collect(Collectors.toList());

        brokerController.unSubscribe(clientId,mqttSubscriptions)
                .onFailure(mqttContext::handleException)
                .onSuccess(v->mqttContext.unSubscribeAcknowledge(message.getPacketIdentifier(), reasonCodes, null, null));


    }


    /**
     * publish handler
     * @param message
     */
    private void onPublish( MqttPublishMessage message){
        // auth
        String clientId = mqttContext.session().clientIdentifier();
        authenticator.authorizePub(clientId, message.getTopic())
                .onFailure(t-> logger.debug("authorize client:{} publish failed  ", clientId))
                .onFailure(mqttContext::handleException)
                .onSuccess(authResult->{
                    List<StringPair> userProperty = authResult.getPairList();
                    if (authResult.getObject()) {
                        //
                        MessageContext messageContext=new MessageContext(new DispatcherMessage().fromPublishMessage(message))
                                .setExpiryTimeStamp(message.getMessageExpiryInterval(),brokerController.getMqttConfig().getLong(MQTT_MAX_MESSAGE_EXPIRY_INTERVAL))
                                .setClientId(clientId);


                        if (message.getQos() == MqttQoS.EXACTLY_ONCE) {
                            session.packetId(PacketIdActionCommand.addPacketId(clientId, message.getPacketId()))
                                    .onFailure(mqttContext::handleException)
                                    .onSuccess(v->{
                                        mqttContext.publishReceived(message.getPacketId(), ReasonCode.SUCCESS, userProperty, null);
                                        brokerController.dispatcherClientMessage(messageContext);
                                    });
                        }else {

                            if (message.getQos() == MqttQoS.AT_LEAST_ONCE) {
                                mqttContext.publishAcknowledge(message.getPacketId(), ReasonCode.SUCCESS, userProperty, null);
                            }

                            brokerController.dispatcherClientMessage(messageContext);
                        }


                    } else {
                        if (message.getQos() == MqttQoS.AT_LEAST_ONCE) {
                            mqttContext.publishAcknowledge(message.getPacketId(), ReasonCode.NOT_AUTHORIZED, userProperty, null);
                        } else if (message.getQos() == MqttQoS.EXACTLY_ONCE) {
                            mqttContext.publishReceived(message.getPacketId(), ReasonCode.NOT_AUTHORIZED, userProperty, null);
                        }
                    }
                });
    }

    private void onClose(Void v){
        this.eventController.cancel();
        if (!mqttContext.isAccept())
            return;

        MqttSession session = mqttContext.session();
        String clientId = session.clientIdentifier();
        session.setExpiryTime();
        MqttWill will = session.will();

        //save expiry Timestamp
        if (mqttContext.isTakenOver()||mqttContext.sessionExpiryInterval() == 0) {
            brokerController.clearSubscribe(clientId);
            //delete context
            ClientController clientController = brokerController.get(clientId);
            if (clientController!=null&&clientController.getMqttContext().id().equals(mqttContext.id())){
                brokerController.remove(clientId);
            }
        }

        CloseSessionCommand closeSessionCommand = new CloseSessionCommand(clientId)
                .setSessionExpiryInterval(mqttContext.sessionExpiryInterval())
                .setDisconnect(mqttContext.isDisConnect())
                .setTakenOver(mqttContext.isTakenOver())
                .setWill(will.isWillFlag())
                .setWillDelayInterval(will.getWillDelayInterval());
        if (will.isWillFlag()){
            closeSessionCommand
                    .setWillDelayInterval(will.getWillDelayInterval())
                    .setWillMessage(new DispatcherMessage().fromWill(will));
        }

        this.session.closeSession(closeSessionCommand);


    }


}
