package com.stormpx.dispatcher.api;

import com.stormpx.dispatcher.MessageContext;
import com.stormpx.dispatcher.command.ClientAcceptCommand;
import com.stormpx.dispatcher.command.SubscriptionsCommand;
import com.stormpx.dispatcher.command.UnSubscriptionsCommand;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class Dispatcher extends DK {
    public final static String CLIENT_SESSION_ACCEPT="_client_session_accept_";
    public final static String MESSAGE_DISPATCHER="_message_dispatcher_";
    public final static String TOPIC_SUBSCRIBE="_topic_subscribe_";
    public final static String TOPIC_UNSUBSCRIBE="_topic_unSubscribe_";

    public Dispatcher(Vertx vertx) {
        super(vertx);
    }

    public Future<Boolean> sessionAccept(ClientAcceptCommand command){
        return super.<Boolean>request(CLIENT_SESSION_ACCEPT,command)
                .map(r->r==null?false:r);
    }


    public void dispatcherMessage(MessageContext messageContext){
        vertx.eventBus().send(MESSAGE_DISPATCHER, messageContext);
    }


    public Future<Void> subscribeTopic(SubscriptionsCommand command){
        return request(TOPIC_SUBSCRIBE,command);
    }

    public Future<Void> unSubscribeTopic(UnSubscriptionsCommand command ){
        return request(TOPIC_UNSUBSCRIBE,command);
    }




}
