package com.stormpx.server;

import io.vertx.core.Future;
import io.vertx.core.Handler;

public interface MqttServer {


    MqttServer exceptionHandler(Handler<Throwable> handler);

    MqttServer handler(Handler<MqttContext> handler);

    MqttConnectionHolder holder();

    Future<Void> listen();

    Future<Void> wsListen();

    Future<Void> close();

}
