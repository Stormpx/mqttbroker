package com.stormpx.cluster;


import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.net.Request;
import com.stormpx.cluster.net.Response;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.streams.ReadStream;
import io.vertx.core.streams.WriteStream;

public interface StateService {



    Future<Void> init(MqttCluster mqttCluster);

    Future<Response> handle(RpcMessage rpcMessage);


    void firePendingEvent(String leaderId);

    void applyLog(LogEntry logEntry);


    void loadSnapshot(ReadStream<Buffer> readStream);

    void writeSnapshot(WriteStream<Buffer> writeStream);

}
