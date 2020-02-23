package com.stormpx;

import com.stormpx.auth.AuthResult;
import com.stormpx.auth.Authenticator;
import com.stormpx.message.*;
import com.stormpx.ex.SharedSubscriptionsNotSupportedException;
import com.stormpx.ex.SubscriptionIdNotSupportedException;
import com.stormpx.ex.WildcardSubscriptionsNotSupportedException;
import com.stormpx.kit.*;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.MqttVersion;
import com.stormpx.mqtt.ReasonCode;
import com.stormpx.server.*;
import com.stormpx.store.MessageLink;
import com.stormpx.store.DataStorage;
import io.netty.handler.codec.mqtt.MqttConnectReturnCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.PemKeyCertOptions;
import io.vertx.core.shareddata.LocalMap;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

import static com.stormpx.Constants.*;

public class MqttBrokerVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger("mqttBroker");

    private MqttServer mqttServer;

    private TopicFilter topicFilter;

    protected DataStorage dataStorage;
    protected Dispatcher dispatcher;

    protected Authenticator authenticator;

    private TimeoutStream willTimer;

    private JsonObject mqttConfig;

    private Map<String,TimeoutStream> willTimerMap;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        onConfigRefresh(config());
        vertx.eventBus().<JsonObject>localConsumer(":config_change::",message->{
           onConfigRefresh(message.body());
        });
        willTimerMap=new HashMap<>();
        this.dispatcher=new Dispatcher(vertx);
        this.dataStorage=new DataStorage(vertx);
        initAuth()
                .compose(v->startServer())
                .setHandler(startFuture);
    }



    protected void onConfigRefresh(JsonObject config) {
        this.mqttConfig=config.getJsonObject("mqtt",new JsonObject());
    }


    protected Future<Void> initAuth(){
        ServiceLoader<Authenticator> authenticators = ServiceLoader.load(Authenticator.class);
        String auth = config().getString(Constants.AUTH, "anonymous");
        authenticators.forEach(authenticator ->{
            if (authenticator.name().equals(auth)){
                this.authenticator=authenticator;
            }
        } );
        if (this.authenticator!=null){
            return this.authenticator.init(vertx,config());
        }
        return Future.failedFuture("can't find authenticator :"+auth);
    }


    private Future<Void> startServer(){

        MqttServerOption mqttServerOption = new MqttServerOption();

        mqttServerOption.setTcpNoDelay(config().getBoolean(Constants.TCP_NO_DELAY,false));
        mqttServerOption.setSni(config().getBoolean(Constants.SNI,false));


        JsonObject tcpJson = config().getJsonObject(TCP, new JsonObject());
        boolean tcp=tcpJson.getBoolean(Constants.ENABLE,true);
        if (tcp){
            JsonArray jsonArray = tcpJson.getJsonArray(KEY_CERT);
            if (jsonArray!=null){
                PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions();
                J.toJsonStream(jsonArray)
                        .forEach(json->{
                            String keyPath = json.getString(KEY_FILE);
                            String certPath = json.getString(CERT_FILE);
                            if (keyPath!=null&&certPath!=null){
                                //log control
                                LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap("storm");
                                localMap.computeIfAbsent("tcp"+keyPath+certPath,k->{
                                    logger.info("mqtt tcp key_path:{} cert_path:{}",keyPath,certPath);
                                    return keyPath+certPath;
                                });

                                pemKeyCertOptions.addKeyPath(keyPath);
                                pemKeyCertOptions.addCertPath(certPath);
                            }
                        });
                mqttServerOption.setTcpPemKeyCertOptions(pemKeyCertOptions);
            }
            mqttServerOption.setTcpSsl(tcpJson.getBoolean(SSL,false));
            mqttServerOption.setTcpHort(tcpJson.getString(Constants.HOST,"0.0.0.0"));
            mqttServerOption.setTcpPort(tcpJson.getInteger(Constants.PORT, mqttServerOption.isTcpSsl()?8883:1883));
        }

        JsonObject wsJson = config().getJsonObject(WS, new JsonObject());
        Boolean ws = wsJson.getBoolean(Constants.ENABLE, false);
        if (ws){
            JsonArray jsonArray = wsJson.getJsonArray(KEY_CERT);
            if (jsonArray!=null){
                PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions();
                J.toJsonStream(jsonArray)
                        .forEach(json->{
                            String keyPath = json.getString(KEY_FILE);
                            String certPath = json.getString(CERT_FILE);
                            if (keyPath!=null&&certPath!=null){
                                //log control
                                LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap("storm");
                                localMap.computeIfAbsent("ws"+keyPath+certPath,k->{
                                    logger.info("mqtt ws key_path:{} cert_path:{}",keyPath,certPath);
                                    return keyPath+certPath;
                                });
                                pemKeyCertOptions.addKeyPath(keyPath);
                                pemKeyCertOptions.addCertPath(certPath);
                            }
                        });
                mqttServerOption.setWsPemKeyCertOptions(pemKeyCertOptions);
            }
            mqttServerOption.setWsSsl(wsJson.getBoolean(SSL,false));
            mqttServerOption.setWsHort(wsJson.getString(Constants.HOST,"0.0.0.0"));
            mqttServerOption.setWsPort(wsJson.getInteger(Constants.PORT, mqttServerOption.isTcpSsl()?8883:1883));
            mqttServerOption.setWsPath(wsJson.getString(Constants.PATH,"/mqtt"));
        }

        this.mqttServer = new MqttServerImpl(vertx,mqttServerOption);
        this.topicFilter=new TopicFilter();

        EventBus eventBus = vertx.eventBus();
        eventBus.<UnSafeJsonObject>consumer("_mqtt_message_dispatcher")
                .handler(message->{
                    JsonObject body = message.body().getJsonObject();

                    dispatcherMessage(body);
                });

        eventBus.<JsonObject>consumer("_mqtt_session_taken_over")
                .handler(message->{
                    JsonObject body = message.body();
                    String clientId = body.getString("clientId");
                    String id = body.getString("id");
                    boolean sessionEnd = body.getBoolean("sessionEnd", false);
                    MqttContext mqttContext = this.mqttServer.holder().get(clientId);
                    TimeoutStream timeoutStream = willTimerMap.remove(clientId);
                    if (timeoutStream!=null)
                        timeoutStream.cancel();

                    if (mqttContext !=null){
                        if (id!=null){
                            if (mqttContext.id().equals(id))
                                return;
                        }
                        mqttServer.holder().remove(clientId);
                        mqttContext.takenOver(sessionEnd);
                    }
                });



        mqttServer.exceptionHandler(t->logger.error("",t))
                .handler(mqttContext->{
                    logger.debug("client:{} try connect",mqttContext.session().clientIdentifier());
                    mqttContext.exceptionHandler(t->{

                        logger.info(mqttContext.session().clientIdentifier()+" ex:{} message:{}",t.getClass().getName(),t.getMessage());
                        logger.error("ex detail",t);
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
                                        handleConnection(mqttContext);
                                    }else{
                                        if (mqttContext.version()==MqttVersion.MQTT_3_1_1) {
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

                });

        Future<Void> future=Future.succeededFuture();
        if (tcp){
            future=mqttServer.listen();
        }
        if (ws){
            future=future.compose(v->mqttServer.wsListen());
        }

        return future.map(v->{
           /* this.willTimer=vertx.periodicStream(new Random().nextInt(3000))
                .voteHandler(id-> processTimeoutWill());*/
            return null;
        });
    }

    private boolean trySetOption(MqttContext mqttContext){
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

        Long maxSessionExpiryInterval = mqttConfig.getLong(MQTT_MAX_SESSION_EXPIRY_INTERVAL);
        if (maxSessionExpiryInterval!=null&&mqttContext.sessionExpiryInterval()>maxSessionExpiryInterval) {
            mqttContext.setSessionExpiryInterval(maxSessionExpiryInterval);
        }
        return true;
    }



    /**
     * auth success
     * set subscribeHandler unSubscribeHandler publishHandler
     * publishReleaseHandler publishAcknowledgeHandler publishReceiveHandler publishCompleteHandler disconnectHandler closeHandler
     * and handle cleanSession
     * @param mqttContext
     */
    private void handleConnection(MqttContext mqttContext){

        var ref=new Object(){
            TimeoutStream updateSessionExpiryStream;
            MessageConsumer<UnSafeJsonObject> messageConsumer;
        };
        mqttContext.subscribeHandler(message -> onSubscribe(mqttContext,message))
                .unSubscribeHandler(message -> onUnSubscribe(mqttContext,message))
                .publishHandler(message -> onClientPublish(mqttContext, message))
                .publishReleaseHandler(id -> dataStorage.removePacketId(mqttContext.session().clientIdentifier(), id))
                .publishAcknowledgeHandler(id -> dataStorage.release(mqttContext.session().clientIdentifier(), id))
                .publishReceiveHandler(id -> dataStorage.receive(mqttContext.session().clientIdentifier(), id))
                .publishCompleteHandler(id -> dataStorage.release(mqttContext.session().clientIdentifier(), id))
                .disconnectHandler(message -> {
                    logger.info("client:{} disconnect userProperty:{}", mqttContext.session().clientIdentifier(), message.getUserProperty());
                    dataStorage.dropWillMessage(mqttContext.session().clientIdentifier());
                }).closeHandler(v -> {
                    if (ref.updateSessionExpiryStream != null)
                        ref.updateSessionExpiryStream.cancel();
                    if (ref.messageConsumer!=null)
                        ref.messageConsumer.unregister();
                    onClose(mqttContext);
                });



        String clientId = mqttContext.session().clientIdentifier();
        ref.messageConsumer=vertx.eventBus().<UnSafeJsonObject>localConsumer("_"+clientId+"_"+mqttContext.id()+"message_")
                .handler(message->{
                    try {
                        JsonObject body = message.body().getJsonObject();
                        String id = body.getString("id");
                        if (id==null){
                            Integer packetId = body.getInteger("packetId");
                            mqttContext.publishRelease(packetId,ReasonCode.SUCCESS,null,null);
                        }else {
                            sendToClient(mqttContext, body);
                        }
                    } catch (Exception e) {
                        mqttContext.handleException(e);
                    }
                });
        handleCleanSession(mqttContext)
                .onFailure(t->{
                    logger.error("client:"+clientId+" fetch session fail :{}",t.getMessage());
                    mqttContext.handleException(t);
                })
                .compose(b-> handleSessionPresent(mqttContext,b))
                .onSuccess(b->{
                    if (mqttContext.sessionExpiryInterval()>1) {
                        ref.updateSessionExpiryStream=vertx.periodicStream((mqttContext.sessionExpiryInterval()-1)*1000)
                                .handler(id->
                                        dataStorage.setExpiryTimestamp(clientId,Instant.now().getEpochSecond()+ mqttContext.sessionExpiryInterval()));
                    }
                    mqttServer.holder().add(mqttContext);
                    logger.info("client:{} accpet version:{} sessionExpiryInterval:{} keepalive:{}",mqttContext.session().clientIdentifier(),mqttContext.version(),mqttContext.sessionExpiryInterval(),mqttContext.keepAlive());
                    mqttContext.accept(b);
                    dispatcher.sessionAccept(clientId,!b);
                });
    }



    private Future<Boolean> handleSessionPresent(MqttContext mqttContext, boolean sessionPresent){
        String clientId = mqttContext.session().clientIdentifier();
        takenOverConnection(mqttContext.id(),clientId,!sessionPresent);
        if (sessionPresent){

            return addUnacknowledgedPacketId(mqttContext)
                    .onSuccess(v->{
                        writeUnFinishMessage(mqttContext);
                        subscribeByClientId(mqttContext);
                    })
                    .map(true);

        }else {
            //session expiry
            topicFilter.clearSubscribe(clientId);
            dataStorage.clearSession(clientId);
            return Future.succeededFuture(false);
        }
    }

    private Future<Boolean> handleCleanSession(MqttContext mqttContext){
        String clientId = mqttContext.session().clientIdentifier();
        if (mqttContext.isCleanSession()){
            //fetch will
            return dataStorage.fetchWillMessage(clientId)
                    //if will is null the client may still connect
                    .onSuccess(json-> tryPublishWill(clientId,json))
                    .map(false);
        }else{
            return dataStorage.getExpiryTimestamp(clientId)
                    .map(expiryTimestamp-> {
                        if (expiryTimestamp==null)
                            return false;
                        return Instant.now().getEpochSecond()<expiryTimestamp;
                    });
        }
    }


    private void takenOverConnection(String newId,String clientId,boolean sessionEnd){
        MqttContext connection = mqttServer.holder().get(clientId);
        if (connection!=null){
            mqttServer.holder().remove(clientId);
            TimeoutStream timeoutStream = willTimerMap.remove(clientId);
            if (timeoutStream!=null)
                timeoutStream.cancel();
            connection.takenOver(sessionEnd);
        }else{
            vertx.eventBus().publish("_mqtt_session_taken_over",new JsonObject().put("id",newId).put("clientId",clientId).put("sessionEnd",sessionEnd));
        }
    }

    /**
     * check option
     * @param message subscribe message
     */
    private void verifySubscribe(MqttSubscribeMessage message){
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
            if (!wildcard&&TopicUtil.containWildcard(topicFilter))
                throw new WildcardSubscriptionsNotSupportedException("topic: "+topicFilter+" wildcard not supported");
            if (!sharedSubscriptionAvailable&&TopicUtil.isShareTopicFilter(topicFilter))
                throw new SharedSubscriptionsNotSupportedException("topic: "+topicFilter+" share subscriptions not supported");
        }

    }
    /**
     * Subscribe voteHandler
     * @param mqttContext
     * @param message
     */
    private void onSubscribe(MqttContext mqttContext,MqttSubscribeMessage message){
        try {
            verifySubscribe(message);
            List<MqttSubscription> mqttSubscriptions = message.getMqttSubscriptions();
            //auth
            authenticator.authorizeSub(mqttContext.session().clientIdentifier(), mqttSubscriptions, message.getUserProperty()).setHandler(ar -> {
                if (ar.succeeded()) {
                    try {
                        AuthResult<List<ReasonCode>> authResult = ar.result();
                        List<ReasonCode> reasonCodes = authResult.getObject();
                        List<String> topic = new ArrayList<>();
                        List<MqttSubscription> subscriptions = new ArrayList<>();
                        int i = 0;
                        for (ReasonCode reasonCode : reasonCodes) {
                            MqttSubscription mqttSubscription = mqttSubscriptions.get(i++);
                            if (reasonCode.byteValue() >= 0x03) {
                                logger.debug("client:{} subscribe topic:{} qos:{} fail reason:{}", mqttContext.session().clientIdentifier(), mqttSubscription.getTopicFilter(), mqttSubscription.getQos(), reasonCode.name().toLowerCase());
                                continue;
                            }
                            boolean isShare = TopicUtil.isShareTopicFilter(mqttSubscription.getTopicFilter());
                            if (!isShare) {
                                switch (mqttSubscription.getRetainHandling()) {
                                    case SEND_MESSAGES_AT_THE_TIME:
                                        topic.add(mqttSubscription.getTopicFilter());
                                        break;
                                    case NOT_EXIST_SEND_MESSAGES_AT_THE_TIME:
                                        if (topicFilter.subscribed(mqttSubscription.getTopicFilter(), mqttContext.session().clientIdentifier())) {
                                            topic.add(mqttSubscription.getTopicFilter());
                                        }
                                        break;
                                }
                            }
                            MqttQoS mqttQoS = MqttQoS.valueOf(reasonCode.byteValue());
                            mqttSubscription.setQos(mqttQoS);
                            subscriptions.add(mqttSubscription);
                            logger.debug("client:{} subscribe topic:{} qos:{} success", mqttContext.session().clientIdentifier(), mqttSubscription.getTopicFilter(), mqttSubscription.getQos());
                            topicFilter.subscribe(mqttSubscription.getTopicFilter(), mqttContext.session().clientIdentifier(), mqttQoS, mqttSubscription.isNoLocal(), mqttSubscription.isRetainAsPublished(), message.getSubscriptionIdentifier());
                        }
                        //get retain message
                        if (!topic.isEmpty()) {
                            String address = UUID.randomUUID().toString();
                            MessageConsumer<UnSafeJsonObject> messageConsumer = vertx.eventBus().localConsumer(address);
                            messageConsumer.handler(msg->{
                                JsonObject jsonObject = msg.body().getJsonObject();
                                mqttSubscriptions.stream().filter(mqttSubscription -> TopicUtil.matches(mqttSubscription.getTopicFilter(), jsonObject.getString("topic")))
                                        .max(Comparator.comparingInt(m -> m.getQos().value()))
                                        .ifPresent(mqttSubscription -> {
                                            if (message.getSubscriptionIdentifier() != 0)
                                                jsonObject.put("subscriptionId", Collections.singletonList(message.getSubscriptionIdentifier()));

                                            jsonObject.put("qos", Math.min(mqttSubscription.getQos().value(), jsonObject.getInteger("qos")));
                                            jsonObject.put("retain", true);
                                            sendToClient(mqttContext, jsonObject);
                                        });
                            });
                            dispatcher.matchRetainMessage(address,topic);
                        }
                        //store subscribe
                        if (!subscriptions.isEmpty()) {
                            dispatcher.subscribeTopic(mqttContext.session().clientIdentifier(), subscriptions, message.getSubscriptionIdentifier());
                        }
                        //ack
                        mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), reasonCodes, authResult.getPairList(), null);
                    } catch (Exception e) {
                        mqttContext.handleException(e);
                    }

                } else {
                    logger.info("client:{} authorize subscribe fail", mqttContext.session().clientIdentifier());
                    mqttContext.handleException(ar.cause());
                }
            });
        } catch (SubscriptionIdNotSupportedException e) {
            mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), message.getMqttSubscriptions().stream().map(s -> ReasonCode.SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED).collect(Collectors.toList()), null, null);
        } catch (WildcardSubscriptionsNotSupportedException e) {
            mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), message.getMqttSubscriptions().stream().map(s -> ReasonCode.WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED).collect(Collectors.toList()), null, null);
        } catch (SharedSubscriptionsNotSupportedException e) {
            mqttContext.subscribeAcknowledge(message.getPacketIdentifier(), message.getMqttSubscriptions().stream().map(s -> ReasonCode.SHARED_SUBSCRIPTIONS_NOT_SUPPORTED).collect(Collectors.toList()), null, null);
        }
    }


    /**
     * UnSubscribe voteHandler
     * @param mqttContext
     * @param message
     */
    private void onUnSubscribe(MqttContext mqttContext, MqttUnSubscribeMessage message){
        List<String> mqttSubscriptions = message.getMqttSubscriptions();
        String clientId = mqttContext.session().clientIdentifier();
        List<ReasonCode> reasonCodes = mqttSubscriptions.stream().peek(s -> topicFilter.unSubscribe(s, clientId)).peek(s -> logger.debug("client:{} unSubscribe topic:{}", clientId, s)).map(s -> ReasonCode.SUCCESS).collect(Collectors.toList());

        dispatcher.unSubscribeTopic(mqttContext.session().clientIdentifier(), mqttSubscriptions);

        mqttContext.unSubscribeAcknowledge(message.getPacketIdentifier(), reasonCodes, null, null);
    }


    /**
     * publish voteHandler
     * @param mqttContext
     * @param message
     */
    private void onClientPublish(MqttContext mqttContext, MqttPublishMessage message){
        // auth
        boolean isQos0 = message.getQos() == MqttQoS.AT_MOST_ONCE;
        String clientId = mqttContext.session().clientIdentifier();

        authenticator.authorizePub(clientId, message.getTopic()).setHandler(aar -> {
            if (aar.succeeded()) {
                AuthResult<Boolean> authResult = aar.result();
                List<StringPair> userProperty = authResult.getPairList();
                if (authResult.getObject()) {
                    logger.debug("clientId:{} publish message topic:{} qos:{} retain:{}", clientId, message.getTopic(), message.getQos().value(), message.isRetain());
                    JsonObject result = message.toJson().put("clientId", clientId);
                    if (!isQos0) {
                        String id = UUID.randomUUID().toString().replaceAll("-", "");
                        result.put("id", id);
                    }

                    //set min expiryTimestamp
                    Long messageExpiryInterval = mqttConfig.getLong(MQTT_MAX_MESSAGE_EXPIRY_INTERVAL);
                    if (messageExpiryInterval == null || (message.getMessageExpiryInterval() != null && messageExpiryInterval >= message.getMessageExpiryInterval())) {
                        messageExpiryInterval = message.getMessageExpiryInterval();
                    }
                    if (messageExpiryInterval != null) {
                        result.put("expiryTimestamp", Instant.now().getEpochSecond() + messageExpiryInterval);
                    }

                    if (message.getQos() == MqttQoS.AT_LEAST_ONCE) {
                        mqttContext.publishAcknowledge(message.getPacketId(), ReasonCode.SUCCESS, userProperty, null);
                    } else if (message.getQos() == MqttQoS.EXACTLY_ONCE) {
                        dataStorage.addPacketId(clientId, message.getPacketId());
                        mqttContext.publishReceived(message.getPacketId(), ReasonCode.SUCCESS, userProperty, null);
                    }
                    //dispatcher
                    dispatcherEvent(result);

                } else {
                    if (message.getQos() == MqttQoS.AT_LEAST_ONCE) {
                        mqttContext.publishAcknowledge(message.getPacketId(), ReasonCode.NOT_AUTHORIZED, userProperty, null);
                    } else if (message.getQos() == MqttQoS.EXACTLY_ONCE) {
                        mqttContext.publishReceived(message.getPacketId(), ReasonCode.NOT_AUTHORIZED, userProperty, null);
                    }
                }
            } else {
                logger.info("authorize client:{} fail publish ", clientId);
                mqttContext.handleException(aar.cause());
            }
        });
    }

    private void onClose(MqttContext mqttContext){
        MqttSession session = mqttContext.session();
        String clientId = session.clientIdentifier();
        session.setExpiryTime();
        MqttWill will = session.will();

        if (mqttContext.isTakenOver()) {
            //remove connection
            topicFilter.clearSubscribe(mqttContext.session().clientIdentifier());
            if (!will.isWillFlag() || (mqttContext.sessionExpiryInterval() != 0 && will.getWillDelayInterval() != 0))
                return;

            //publish message
            tryPublishWill(clientId, will.toJson());
            return;
        }

//        mqttServer.holder().remove(mqttContext.session().clientIdentifier());

        //save expiry Timestamp
        if (mqttContext.sessionExpiryInterval() != 0)
            dataStorage.setExpiryTimestamp(clientId, session.expiryTime());
        else {
            topicFilter.clearSubscribe(clientId);
            dataStorage.clearSession(clientId);
        }
        if (mqttContext.isDisConnect()) return;

        logger.debug("client:{} lost", clientId);

        if (will.isWillFlag()) {
            JsonObject json = will.toJson();

            Long delayInterval = json.getLong("delayInterval", 0L);
            delayInterval = Math.min(delayInterval, mqttContext.sessionExpiryInterval());

            if (delayInterval == 0) {
                tryPublishWill(clientId, json);
            } else {
                TimeoutStream willTimeoutStream = vertx.timerStream(delayInterval);
                willTimerMap.put(clientId, willTimeoutStream);
                willTimeoutStream.handler(id -> {
                    TimeoutStream timeoutStream = willTimerMap.remove(clientId);
                    if (timeoutStream != null) {
                        tryPublishWill(clientId, json);
                    }
                });
                dataStorage.storeWillMessage(mqttContext.session().clientIdentifier(), json);
            }
        }

    }

    private void dispatcherEvent(JsonObject message) {
        dispatcher.dispatcherMessage(message);
    }

    private void dispatcherMessage(JsonObject message){
        Long expiryTimestamp = message.getLong("expiryTimestamp");
        Long messageExpiryInterval=null;
        if (expiryTimestamp!=null){
            messageExpiryInterval=expiryTimestamp-Instant.now().getEpochSecond();
            if (messageExpiryInterval<=0){
                //expiry
                return;
            }
        }
        String id = message.getString("id");
        String clientId = message.getString("clientId");
        String topic = message.getString("topic");
        MqttQoS qos = MqttQoS.valueOf(message.getInteger("qos"));
        boolean retain = message.getBoolean("retain");
        JsonArray shareTopics= (JsonArray) message.remove("shareTopics");
        if (shareTopics==null)
            shareTopics=J.EMPTY_ARRAY;

        Set<String> shareTopicSet = shareTopics.stream().map(Object::toString).collect(Collectors.toSet());

        MqttConnectionHolder holder = mqttServer.holder();
        //find match  subscriber
        Collection<TopicFilter.SubscribeInfo> matches = topicFilter.matches(topic);
        if (matches.isEmpty()&&!retain){
            return;
        }

        for (TopicFilter.SubscribeInfo subscribeInfo : matches) {
            MqttContext mqttContext = holder.get(subscribeInfo.getClientId());
            if (mqttContext == null) continue;

            TopicFilter.Entry maxQosEntry=null;
            JsonArray subscriptionIds=new JsonArray();

            for (TopicFilter.Entry e : subscribeInfo.getAllMatchSubscribe()) {
                //if isShare subscribe publish
                if (e.isShare()) {
                    if (!shareTopicSet.contains(e.getTopicFilterName()))
                        continue;
                    //share subscribe get lock
                    vertx.sharedData().getLockWithTimeout(e.getTopicFilterName()+id,40, ar->{
                        if (ar.succeeded()){
                            logger.debug("client:{} get lock success ",clientId);

                            sendToClient(mqttContext,message.copy()
                                    .put("qos",Math.min(qos.value(),e.getMqttQoS().value()))
                                    .put("retain", e.isRetainAsPublished() && retain)
                                    .put("subscriptionId",e.getSubscriptionIdentifier()!=0?Collections.singletonList(e.getSubscriptionIdentifier()): J.EMPTY_ARRAY));
                            //                            vertx.setTimer(40,v->ar.result().release());

                        }else{
                            logger.debug("client:{} get lock fail cause:{}",clientId,ar.cause());
                        }
                    });

                    continue;
                }

                if (maxQosEntry==null||e.getMqttQoS().value()>maxQosEntry.getMqttQoS().value()) {
                    maxQosEntry = e;
                }

                if (e.getSubscriptionIdentifier()!=0) {
                    subscriptionIds.add(e.getSubscriptionIdentifier());
                }
            }

            if (maxQosEntry!=null) {
                if (maxQosEntry.isNoLocal() && clientId.equals(mqttContext.session().clientIdentifier()))
                    continue;

                sendToClient(mqttContext,message.copy()
                        .put("qos",Math.min(qos.value(),maxQosEntry.getMqttQoS().value()))
                        .put("retain", maxQosEntry.isRetainAsPublished() && retain)
                        .put("subscriptionId",subscriptionIds));

            }

        }

    }

    private void sendToClient(MqttContext mqttContext, JsonObject message){
        if (mqttContext ==null )
            return;
        Long expiryTimestamp = message.getLong("expiryTimestamp");
        Long messageExpiryInterval=null;
        if (expiryTimestamp!=null){
            messageExpiryInterval=expiryTimestamp-Instant.now().getEpochSecond();
            if (messageExpiryInterval<=0){
                //expiry
                return;
            }
        }

        String id = message.getString("id");
        Integer packetId = message.getInteger("packetId");
        String topic = message.getString("topic");
        MqttQoS qos = MqttQoS.valueOf(message.getInteger("qos"));
        boolean retain = message.getBoolean("retain");
        boolean dup = message.getBoolean("dup", false);
        Buffer payload = Buffer.buffer(message.getBinary("payload"));


        JsonArray subscriptionId = message.getJsonArray("subscriptionId");
        List<Integer> list=null;
        if (subscriptionId!=null){
            list=subscriptionId.stream().map(o->(Integer)o).collect(Collectors.toList());

        }
        if (mqttContext.isClose()){
            if (Instant.now().getEpochSecond()<=mqttContext.session().expiryTime()&&packetId==null&&qos!=MqttQoS.AT_MOST_ONCE){
                dataStorage.link(MessageLink.create(id, mqttContext.session().clientIdentifier(),null,retain,qos, list));
            }
            return;
        }

        MqttBrokerMessage mqttBrokerMessage = new MqttBrokerMessage().setTopic(topic)
                .setQos(qos).setRetain(retain).setDup(dup).setPayload(payload)
                .setMessageExpiryInterval(messageExpiryInterval);

        JsonArray properties = message.getJsonArray("properties");
        List<MqttProperties> propertiesList=Collections.emptyList();
        if (properties !=null){
            propertiesList=properties.stream()
                    .map(o->(JsonObject)o)
                    .map(MqttProperties::fromJson)
                    .collect(Collectors.toList());

        }

        if (packetId!=null){
            mqttBrokerMessage.setPacketId(packetId);
        }
        mqttBrokerMessage.setProperties(propertiesList);
        mqttBrokerMessage.setSubscriptionId(list);
        List<Integer> finalList = list;
        mqttContext.publish(mqttBrokerMessage)
                .setHandler(ar->{
                   if (ar.succeeded()){
                       Integer pid = ar.result();
                       if (packetId==null&&qos!=MqttQoS.AT_MOST_ONCE){
                           dataStorage.link(MessageLink.create(id, mqttContext.session().clientIdentifier(),pid,retain,qos, finalList));
                       }
                   }else{
                       logger.error("client:"+mqttContext.session().clientIdentifier()+" publish message fail cause",ar.cause());
                   }
                });

    }

    private Future<Void> subscribeByClientId(MqttContext mqttContext){
        String clientId = mqttContext.session().clientIdentifier();
        return dataStorage.fetchSubscription(clientId)
                .setHandler(ar->{
                   if (ar.succeeded()){
                       JsonArray array = ar.result();
                       if (array==null)
                           return;
                        array.stream()
                                .filter(o->o instanceof JsonObject)
                                .map(o->(JsonObject)o)
                                .forEach(json->{
                                    topicFilter.subscribe(json.getString("topicFilter"),clientId,
                                            MqttQoS.valueOf(json.getInteger("qos")),json.getBoolean("noLocal",false),
                                            json.getBoolean("retainAsPublished",false),json.getInteger("identifier",0));
                                });
                   }else{
                       logger.info("fetch subscription fail :{}",ar.cause().getMessage());
                       mqttContext.handleException(ar.cause());
                   }
                })
                .map((Void)null);
    }

    private Future<Void> writeUnFinishMessage(MqttContext mqttContext){
        String clientId = mqttContext.session().clientIdentifier();
        String id = mqttContext.id();
        String address="_"+clientId+"_"+id+"message_";
        dispatcher.resendMessage(address,clientId);
        return Future.succeededFuture();
        /*return dataStorage.fetchUnReleaseMessage(mqttContext.session().clientIdentifier()).setHandler(ar -> {
            if (ar.succeeded()){
                JsonArray array = ar.result();
                array.stream()
                        .filter(o->o instanceof JsonObject)
                        .map(o->(JsonObject)o)
                        .forEach(json->{
                            if (null == json.getInteger("id")){
                                //resent release
                                Integer packetId = json.getInteger("packetId");
                                mqttContext.publishRelease(packetId,ReasonCode.SUCCESS,null,null);
                            }else{
                                //re publish
                                json.put("dup",true);
                                sendToClient(mqttContext,json);
                            }
                        });
            }else{
                logger.info("fetch unFinish message fail:{} ",ar.cause().getMessage());
                mqttContext.handleException(ar.cause());
            }
        }).map((Void)null)*/
    }

    private Future<Void> addUnacknowledgedPacketId(MqttContext mqttContext){
        return dataStorage.unacknowledgedPacketId(mqttContext.session().clientIdentifier())
                .setHandler(ar->{
                   if (ar.succeeded()){
                       List<Integer> list = ar.result();
                       if (list!=null)
                           list.forEach(mqttContext.session()::addPacketId);
                   }else{
                       logger.info("fetch unacknowledged packetId fail:{} ",ar.cause().getMessage());
                       mqttContext.handleException(ar.cause());
                   }
                }).map((Void)null);
    }


    private Future<Void> tryPublishWill(String clientId,JsonObject willJson){
        if (willJson==null)
            return Future.succeededFuture();
        Long messageExpiryInterval = willJson.getLong("messageExpiryInterval");
        if (messageExpiryInterval!=null) {
            willJson.put("expiryTimestamp", Instant.now().getEpochSecond()+messageExpiryInterval);
        }
        willJson.remove("delayInterval");
        willJson.put("id",UUID.randomUUID().toString().replaceAll("-",""));
        dispatcherEvent(willJson);
        dataStorage.dropWillMessage(clientId);
        /*return dataStorage.storeMessage(willJson)
                .compose(id->{
                    willJson.put("id",id);
                    dispatcherEvent(willJson);
                    dataStorage.dropWillMessage(clientId);
                    return Future.succeededFuture();
                });*/
        return Future.succeededFuture();
    }



    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (mqttServer!=null){
            mqttServer.close();
        }
        if (willTimer!=null){
            willTimer.cancel();
        }
        stopFuture.complete();
    }



}
