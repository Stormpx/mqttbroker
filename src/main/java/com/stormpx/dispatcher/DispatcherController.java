package com.stormpx.dispatcher;

import com.stormpx.dispatcher.api.Dispatcher;
import com.stormpx.dispatcher.api.Center;
import com.stormpx.dispatcher.api.Session;
import com.stormpx.dispatcher.command.*;
import com.stormpx.kit.TopicFilter;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.List;

public class DispatcherController {
    private final static Logger logger= LoggerFactory.getLogger(DispatcherController.class);


    private DispatcherContext dispatcherContext;

    public DispatcherController(DispatcherContext dispatcherContext) {
        this.dispatcherContext = dispatcherContext;
        initSessionApi();
        initDispatcherApi();
    }

    public void initDispatcherApi(){
        Vertx vertx = dispatcherContext.getVertx();
        SessionService sessionService = dispatcherContext.getSessionService();
        MessageService messageService = dispatcherContext.getMessageService();
        TopicFilter topicFilter = dispatcherContext.getTopicFilter();

        vertx.eventBus()
                .<ClientAcceptCommand>localConsumer(Dispatcher.CLIENT_SESSION_ACCEPT)
                .handler(message->{
                    ClientAcceptCommand session = message.body();
                    sessionService.acceptSession(session)
                            .onFailure(logger::error)
                            .onFailure(t->message.fail(500,t.getMessage()))
                            .onSuccess(message::reply);
                });

        vertx.eventBus()
                .<MessageContext>localConsumer(Dispatcher.MESSAGE_DISPATCHER)
                .handler(message->{
                    MessageContext messageContext = message.body();
                    messageService.exchange(messageContext);
                });


        vertx.eventBus()
                .<SubscriptionsCommand>localConsumer(Dispatcher.TOPIC_SUBSCRIBE)
                .handler(message->{
                    SubscriptionsCommand command = message.body();
                    List<MqttSubscription> mqttSubscriptions = command.getMqttSubscriptions();
                    //client save subscribe
                    sessionService.saveSubscription(command.getClientId(),mqttSubscriptions);

                    //verticle subscribe
                    mqttSubscriptions.forEach(
                            s-> topicFilter.subscribe(s.getTopicFilter(),command.getId(),s.getQos(),s.isNoLocal(),s.isRetainAsPublished(),s.getSubscriptionId())
                    );

                    //match retain message and callback
                    messageService.matchRetainMessage(command.getMatchTopics(), msg-> {
                        if (msg!=null)
                            vertx.eventBus().send(command.getAddress(),new MessageContext(msg.setRetain(true)));
                    });
                    message.reply(null);
                });

        vertx.eventBus()
                .<UnSubscriptionsCommand>localConsumer(Dispatcher.TOPIC_UNSUBSCRIBE)
                .handler(message->{
                    UnSubscriptionsCommand command = message.body();
                    //client del subscribe
                    sessionService.delSubscription(command.getClientId(),command.getTopics());


                    //verticle unSubscribe
                    command.getVerticleUnSubscribeTopic().forEach(topic->{
                        boolean anySubscribe = topicFilter.unSubscribe(topic, command.getId());
                        if (dispatcherContext.isCluster()&&!anySubscribe){

                        }
                    });
                    message.reply(null);
                });

    }


    public void initSessionApi(){

        Vertx vertx = dispatcherContext.getVertx();
        SessionService sessionService = dispatcherContext.getSessionService();
        MessageService messageService = dispatcherContext.getMessageService();

        vertx.eventBus()
                .<CloseSessionCommand>localConsumer(Session.CLOSE_SESSION)
                .handler(message->{
                    sessionService.closeSession(message.body());
                    message.reply(null);
                });


        vertx.eventBus()
                .<MessageLink>localConsumer(Session.LINK)
                .handler(message->{
                    MessageLink link = message.body();
                    sessionService.link(link);
                    messageService.modifyRefCnt(link.getId(),1);

                    message.reply(null);
                });

        vertx.eventBus()
                .<PacketIdActionCommand>localConsumer(Session.PACKET_ID)
                .handler(message->{
                    PacketIdActionCommand packetIdActionCommand = message.body();
                    sessionService.packetId(packetIdActionCommand)
                        .onSuccess(id->{
                            if (id!=null)
                                messageService.modifyRefCnt(id,-1);
                        });
                    message.reply(null);
                });

    }

    public void initClusterApi(){
        Vertx vertx = dispatcherContext.getVertx();
        SessionService sessionService = dispatcherContext.getSessionService();
        MessageService messageService = dispatcherContext.getMessageService();


        vertx.eventBus()
                .<String>localConsumer(Center.GET_SESSION)
                .handler(message->{
                    String clientId = message.body();
                    sessionService.getSession(clientId)
                            .onFailure(t->message.fail(500,t.getMessage()))
                            .onSuccess(message::reply);
                });

        vertx.eventBus()
                .<String>localConsumer(Center.GET_MESSAGE)
                .handler(message->{
                    String id = message.body();
                    messageService.getLocalMessage(id)
                            .onFailure(logger::error)
                            .onFailure(t->message.fail(500,t.getMessage()))
                            .onSuccess(message::reply);
                });

        vertx.eventBus()
                .<JsonObject>localConsumer(Center.TAKEN_OVER_SESSION)
                .handler(message->{
                    JsonObject body = message.body();
                    String clientId = body.getString("clientId");
                    Boolean sessionEnd = body.getBoolean("sessionEnd");
                    sessionService.takenOverLocalSession(clientId,null,sessionEnd);
                });

        vertx.eventBus()
                .<MessageContext>localConsumer(Center.DISPATCHER_MESSAGE)
                .handler(message->{
                    MessageContext body = message.body();
                    messageService.exchange(body);
                });

        vertx.eventBus().<String>localConsumer(Center.RESET_SESSION)
                .handler(message->{
                    String clientId = message.body();
                    sessionService.delSession(clientId);
                });
    }

}
