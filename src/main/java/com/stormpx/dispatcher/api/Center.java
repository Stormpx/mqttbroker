package com.stormpx.dispatcher.api;

import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.dispatcher.MessageContext;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class Center extends DK {

    public final static String GET_SESSION ="_cluster_get_session_";
    public final static String GET_MESSAGE ="_cluster_get_message_";
    public final static String TAKEN_OVER_SESSION="_cluster_taken_over_session_";
    public final static String DISPATCHER_MESSAGE="_cluster_dispatcher_message_";
    public final static String RESET_SESSION="_cluster_reset_session_";

    public Center(Vertx vertx) {
        super(vertx);
    }

    public Future<Void> resetSession(String clientId){
        return request(RESET_SESSION,clientId);
    }


    public Future<Buffer> getSession(String clientId) {
        return request(GET_SESSION,clientId);
    }

    public Future<Buffer> getMessage(String id) {
        return request(GET_MESSAGE,id);
    }

    public Future<Boolean> takenOverSession(JsonObject body){
        vertx.eventBus().send(Center.TAKEN_OVER_SESSION, body);
        return Future.succeededFuture(true);
    }

    public Future<Boolean> dispatcher(MessageContext message){
        vertx.eventBus().send(Center.DISPATCHER_MESSAGE, message);
        return Future.succeededFuture(true);
    }

}
