package com.stormpx.dispatcher;

import com.stormpx.dispatcher.command.SubscriptionsCommand;
import com.stormpx.dispatcher.command.UnSubscriptionsCommand;
import com.stormpx.kit.TopicFilter;
import com.stormpx.mqtt.MqttSubscription;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.eventbus.MessageProducer;

import java.util.List;
import java.util.stream.Collectors;

public class SubscriptionServiceImpl implements SubscriptionService {

    protected DispatcherContext dispatcherContext;

    private SubscriptionHook subscriptionHook;

    public SubscriptionServiceImpl(DispatcherContext dispatcherContext) {
        this.dispatcherContext = dispatcherContext;
    }

    public SubscriptionServiceImpl setSubscriptionHook(SubscriptionHook subscriptionHook) {
        this.subscriptionHook = subscriptionHook;
        return this;
    }

    @Override
    public void subscribe(SubscriptionsCommand command) {
        SessionService sessionService = dispatcherContext.getSessionService();
        MessageService messageService = dispatcherContext.getMessageService();
        TopicFilter topicFilter = dispatcherContext.getTopicFilter();
        List<MqttSubscription> mqttSubscriptions = command.getMqttSubscriptions();
        if (command.getClientId()!=null)
            //client save subscribe
            sessionService.saveSubscription(command.getClientId(),mqttSubscriptions);

        //verticle subscribe
        List<String> topics=mqttSubscriptions
                .stream()
                .filter(s->topicFilter.subscribe(s.getTopicFilter(),command.getId(), MqttQoS.EXACTLY_ONCE,false,false,0))
                .map(MqttSubscription::getTopicFilter)
                .collect(Collectors.toList());

        if (!topics.isEmpty()){
            if (subscriptionHook!=null)
                subscriptionHook.onTopicSubscribe(topics);
        }


        //match retain message and callback
        if (command.getAddress()!=null&&!command.getMatchTopics().isEmpty()) {
//            MessageProducer<MessageContext> messageProducer = dispatcherContext.getVertx().eventBus().sender(command.getAddress());
            messageService.matchRetainMessage(command.getMatchTopics(), msg -> {
                if (msg != null) dispatcherContext.getVertx().eventBus().send(command.getAddress(), new MessageContext(msg.setRetain(true)));
            });
        }
    }




    @Override
    public void unSubscribe(UnSubscriptionsCommand command) {
        SessionService sessionService = dispatcherContext.getSessionService();
        TopicFilter topicFilter = dispatcherContext.getTopicFilter();
        //client del subscribe
        if (command.getClientId()!=null&&command.getTopics()!=null&&!command.getTopics().isEmpty())
            sessionService.delSubscription(command.getClientId(),command.getTopics());


        //verticle unSubscribe
        List<String> topics=command.getVerticleUnSubscribeTopic()
                .stream()
                .filter(topic->!topicFilter.unSubscribe(topic, command.getId()))
                .collect(Collectors.toList());

        if (!topics.isEmpty()&&subscriptionHook!=null) {
            subscriptionHook.onTopicUnSubscribe(topics);
        }
    }

}
