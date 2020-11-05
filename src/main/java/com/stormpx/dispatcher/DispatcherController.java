package com.stormpx.dispatcher;

import com.stormpx.dispatcher.api.Dispatcher;
import com.stormpx.dispatcher.api.Center;
import com.stormpx.dispatcher.api.Session;
import com.stormpx.dispatcher.command.*;
import com.stormpx.kit.Codec;
import com.stormpx.kit.TopicFilter;
import com.stormpx.store.MessageLink;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class DispatcherController {
    private final static Logger logger= LoggerFactory.getLogger(DispatcherController.class);


    private DispatcherContext dispatcherContext;

    public DispatcherController(DispatcherContext dispatcherContext) {
        this.dispatcherContext = dispatcherContext;
        initSessionApi();
        initDispatcherApi();
        initClusterApi();
    }

    public void initDispatcherApi(){
        Vertx vertx = dispatcherContext.getVertx();
        SessionService sessionService = dispatcherContext.getSessionService();
        MessageService messageService = dispatcherContext.getMessageService();
        SubscriptionService subscriptionService = dispatcherContext.getSubscriptionService();
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
                    messageService.dispatcher(messageContext);
                });


        vertx.eventBus()
                .<SubscriptionsCommand>localConsumer(Dispatcher.TOPIC_SUBSCRIBE)
                .handler(message->{
                    SubscriptionsCommand command = message.body();
                    subscriptionService.subscribe(command);
                    message.reply(null);
                });

        vertx.eventBus()
                .<UnSubscriptionsCommand>localConsumer(Dispatcher.TOPIC_UNSUBSCRIBE)
                .handler(message->{
                    UnSubscriptionsCommand command = message.body();
                    subscriptionService.unSubscribe(command);
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
                            .map(Codec::encode)
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
                    body.setFromCluster(true);
                    messageService.dispatcher(body);
                });

        vertx.eventBus().<String>localConsumer(Center.RESET_SESSION)
                .handler(message->{
                    String clientId = message.body();
                    sessionService.delSession(clientId);
                });
    }

}
