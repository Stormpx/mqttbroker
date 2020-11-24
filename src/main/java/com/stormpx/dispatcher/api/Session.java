package com.stormpx.dispatcher.api;

import com.stormpx.dispatcher.command.CloseSessionCommand;
import com.stormpx.dispatcher.command.PacketIdActionCommand;
import com.stormpx.store.MessageLink;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class Session extends DK{

    public final static String CLOSE_SESSION="_close_session_";
    public final static String LINK="_link_message_";
    public final static String PACKET_ID="_packet_id_action_";



    public Session(Vertx vertx) {
        super(vertx);
    }

    public  Future<Void> closeSession(CloseSessionCommand command){
        return request(CLOSE_SESSION,command);
    }


    public Future<Void> link(MessageLink link){
        return request(LINK,link);
    }

    public Future<Void> packetId(PacketIdActionCommand command){
        return request(PACKET_ID, command);
    }

}
