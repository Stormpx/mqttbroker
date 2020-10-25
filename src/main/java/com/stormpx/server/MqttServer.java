package com.stormpx.server;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.json.JsonObject;

public interface MqttServer {


    MqttServer exceptionHandler(Handler<Throwable> handler);

    MqttServer handler(Handler<MqttContext> handler);


    MqttServer setConfig(JsonObject config);

    Future<Void> listen();


    Future<Void> close();

}
