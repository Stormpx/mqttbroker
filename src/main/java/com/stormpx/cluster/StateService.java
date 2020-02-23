package com.stormpx.cluster;


import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.net.Request;
import com.stormpx.cluster.net.Response;
import io.vertx.core.Future;

public interface StateService {



    Future<Void> init(MqttCluster mqttCluster);

    Future<Response> handle(RpcMessage rpcMessage);


    void firePendingEvent(String leaderId);

    void applyLog(LogEntry logEntry);

}
