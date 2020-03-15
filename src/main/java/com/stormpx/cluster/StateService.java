package com.stormpx.cluster;


import com.stormpx.cluster.message.ClusterMessage;
import com.stormpx.cluster.net.ClientRequest;
import com.stormpx.cluster.net.Response;
import com.stormpx.cluster.snapshot.SnapshotContext;
import com.stormpx.cluster.snapshot.SnapshotReader;
import io.vertx.core.Future;

public interface StateService {



    Future<Void> init(MqttCluster mqttCluster);

    void handle(ClientRequest clientRequest);


    void firePendingEvent(String leaderId);

    void applyLog(LogEntry logEntry);


    Future<Void> applySnapshot(SnapshotReader snapshotReader);

    void writeSnapshot(SnapshotContext snapshotContext);

}
