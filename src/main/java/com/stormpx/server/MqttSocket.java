package com.stormpx.server;

import com.stormpx.mqtt.MqttSessionOption;
import com.stormpx.mqtt.packet.MqttPacket;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

public interface MqttSocket {


    static MqttSocket wrapper(NetSocket netSocket, MqttSessionOption sessionOption){
        return new NetSocketWrapper(netSocket,sessionOption);
    }

    static MqttSocket wrapper(Vertx vertx,WebSocketBase webSocketBase, MqttSessionOption sessionOption){
        return new WebSocketWrapper(vertx,webSocketBase, sessionOption);
    }

    MqttSessionOption getSessionOption();

    SocketAddress remoteAddress();

    MqttSocket setKeepAlive(int keepAlive);

    MqttSocket pause();

    MqttSocket resume();

    MqttSocket handler(Handler<MqttPacket> handler);

    MqttSocket writePacket(MqttPacket packet);

    MqttSocket writePacket(MqttPacket packet, Handler<AsyncResult<Void>> handler);

    MqttSocket closeHandler(Handler<Void> handler);

    MqttSocket close();

}
