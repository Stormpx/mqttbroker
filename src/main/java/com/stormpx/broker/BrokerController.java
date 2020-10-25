package com.stormpx.broker;

import com.stormpx.dispatcher.*;
import com.stormpx.dispatcher.api.Dispatcher;
import com.stormpx.dispatcher.command.ClientAcceptCommand;
import com.stormpx.dispatcher.command.SubscriptionsCommand;
import com.stormpx.dispatcher.command.TakenOverCommand;
import com.stormpx.dispatcher.command.UnSubscriptionsCommand;
import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.TopicUtil;
import com.stormpx.mqtt.MqttSubscription;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;


public class BrokerController {
    private final static Logger logger= LoggerFactory .getLogger(BrokerController.class);
    private Vertx vertx;

    private JsonObject mqttConfig;

    private Map<String,ClientController> controllerMap;

    private TopicFilter topicFilter;

    private Dispatcher dispatcher;

    public BrokerController(Vertx vertx) {
        this.vertx = vertx;
        this.dispatcher=new Dispatcher(vertx);
        init();
    }


    private void init(){
        this.topicFilter=new TopicFilter();
        this.controllerMap=new HashMap<>();

        EventBus eventBus = vertx.eventBus();
        eventBus.<MessageContext>consumer(verticleId()+"_mqtt_message_dispatcher")
                .handler(message->{
                    MessageContext body = message.body();

                    dispatcherMessage(body);
                });

        eventBus.<TakenOverCommand>consumer("_mqtt_session_taken_over")
                .handler(message->{
                    TakenOverCommand command = message.body();
                    String clientId = command.getClientId();
                    String id = command.getId();

                    ClientController clientController = get(clientId);
                    if (clientController!=null&&!clientController.getMqttContext().id().equals(id)) {
                        remove(clientId);
                        clientController.getMqttContext().takenOver(command.isSessionEnd());
                    }
                });

    }



    public void add(ClientController clientController) {
        controllerMap.put(clientController.getMqttContext().session().clientIdentifier(), clientController);
    }

    public ClientController get(String clientId) {
        return controllerMap.get(clientId);
    }

    public void remove(String clientId) {
        controllerMap.remove(clientId);
    }



    private String verticleId(){
        return vertx.getOrCreateContext().deploymentID();
    }

    public JsonObject getMqttConfig() {
        return mqttConfig;
    }

    public BrokerController setMqttConfig(JsonObject mqttConfig) {
        this.mqttConfig = mqttConfig;
        return this;
    }




    public void dispatcherClientMessage(MessageContext messageContext) {
        dispatcher.dispatcherMessage(messageContext);
    }

    public void dispatcherMessage(MessageContext messageContext){
        DispatcherMessage message = messageContext.getMessage();
        if (message.isExpiry())
            return;


        String id = messageContext.getId();
        String clientId = messageContext.getClientId();
        String topic = message.getTopic();
        MqttQoS qos = message.getQos();
        boolean retain = message.isRetain();
        Set<String> shareTopics = messageContext.getShareTopics();

        //find match  subscriber
        Collection<TopicFilter.SubscribeMatchResult> matches = topicFilter.matches(topic);
        if (matches.isEmpty()&&!retain){
            return;
        }
        for (TopicFilter.SubscribeMatchResult subscribeMatchResult : matches) {
            ClientController clientController = get(subscribeMatchResult.getClientId());
            if (clientController == null) continue;

            TopicFilter.Entry maxQosEntry=null;
            List<Integer> subscriptionIds=new ArrayList<>();

            for (TopicFilter.Entry e : subscribeMatchResult.getAllMatchSubscribe()) {
                //if isShare subscribe publish
                if (e.isShare()) {
                    if (!shareTopics.contains(e.getTopicFilterName()))
                        continue;
                    //share subscribe publish
                    clientController.send(
                            message.copy()
                                    .setQos(MqttQoS.valueOf(Math.min(qos.value(),e.getMqttQoS().value())))
                                    .setRetain(e.isRetainAsPublished() && retain),
                            e.getSubscriptionIdentifier()!=0?Collections.singletonList(e.getSubscriptionIdentifier()): Collections.emptyList());

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
                if (maxQosEntry.isNoLocal() && clientController.getMqttContext().session().clientIdentifier().equals(clientId))
                    continue;


                clientController.send(
                        message
                                .copy()
                                .setRetain( maxQosEntry.isRetainAsPublished() && retain)
                                .setQos(MqttQoS.valueOf(Math.min(qos.value(), maxQosEntry.getMqttQoS().value()))),
                        subscriptionIds);

            }

        }
    }


    public Future<Boolean> tryAcceptSession(String clientId,boolean cleanSession,String sessionId,String msgAddress){
        return dispatcher.sessionAccept(ClientAcceptCommand.create(clientId, cleanSession, verticleId(),sessionId ,msgAddress));
    }


    /**
     * client subscribe topic and try match retain message
     * @param clientId clientid
     * @param subscriptions
     * @param onMatchRetainMessage
     * @return
     */
    public Future<Void> subscribe(String clientId, List<MqttSubscription> subscriptions, Handler<MessageContext> onMatchRetainMessage){

        List<String> topic=subscriptions.stream()
                .peek(mqttSubscription->topicFilter.subscribe(mqttSubscription.getTopicFilter(), clientId,
                        mqttSubscription.getQos(), mqttSubscription.isNoLocal(), mqttSubscription.isRetainAsPublished(), mqttSubscription.getSubscriptionId()))
                .filter(mqttSubscription -> isInterestRetain(mqttSubscription,clientId))
                .map(MqttSubscription::getTopicFilter)
                .collect(Collectors.toList());

        //get retain message

        String address = UUID.randomUUID().toString();
        if (!topic.isEmpty()){
            MessageConsumer<MessageContext> messageConsumer = vertx.eventBus().localConsumer(address);
            messageConsumer.handler(msg->onMatchRetainMessage.handle(msg.body()));
        }
        //store subscribe
        SubscriptionsCommand command = new SubscriptionsCommand()
                .setId(verticleId())
                .setClientId(clientId)
                .setMqttSubscriptions(subscriptions)
                .setAddress(address)
                .setMatchTopics(topic);
        return dispatcher.subscribeTopic(command);

    }
    public void reSubscribe(String clientId,List<MqttSubscription> subscriptions){

        for (MqttSubscription mqttSubscription : subscriptions) {
            topicFilter.subscribe(mqttSubscription.getTopicFilter(), clientId,
                    mqttSubscription.getQos(), mqttSubscription.isNoLocal(), mqttSubscription.isRetainAsPublished(), mqttSubscription.getSubscriptionId());
        }

    }


    private boolean isInterestRetain(MqttSubscription mqttSubscription, String clientId){
        boolean isShare = TopicUtil.isShareTopicFilter(mqttSubscription.getTopicFilter());
        if (!isShare) {
            switch (mqttSubscription.getRetainHandling()) {
                case SEND_MESSAGES_AT_THE_TIME:
                    return true;
                case NOT_EXIST_SEND_MESSAGES_AT_THE_TIME:
                    if (!topicFilter.subscribed(mqttSubscription.getTopicFilter(),clientId)) {
                        return true;
                    }
                    break;
            }
        }
        return false;
    }


    /**
     * client unsubscribe topic
     * @param clientId
     * @param mqttSubscriptions
     * @return
     */
    public Future<Void> unSubscribe(String clientId,List<String> mqttSubscriptions){
        List<String> list=mqttSubscriptions
                .stream()
                .peek(s -> topicFilter.unSubscribe(s, clientId))
                .peek(s -> logger.info("client:{} unSubscribe topic:{}", clientId, s))
                .filter(s->!topicFilter.anySubscribed(s))
                .collect(Collectors.toList());

        UnSubscriptionsCommand command = new UnSubscriptionsCommand()
                .setId(verticleId())
                .setClientId(clientId)
                .setTopics(mqttSubscriptions)
                .setVerticleUnSubscribeTopic(list);
        return dispatcher.unSubscribeTopic(command);
    }

    public void clearSubscribe(String clientId){
        //todo
        topicFilter.clearSubscribe(clientId);
    }


}
