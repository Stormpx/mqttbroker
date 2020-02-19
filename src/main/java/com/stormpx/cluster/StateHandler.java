package com.stormpx.cluster;


import com.stormpx.cluster.net.Request;
import com.stormpx.cluster.net.Response;
import io.vertx.core.Future;

public interface StateHandler  {

    Future<ClusterState> loadClusterState();

    void handle(Request request);

    void handle(Response response);

    void onSafety(String leaderId);

    void saveState(ClusterState clusterState);

    void saveLog(LogEntry logEntry);

    void delLog(int start,int end);

    void executeLog(LogEntry logEntry);

}
