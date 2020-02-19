package com.stormpx.cluster.net;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

public class ReadIndexRequest {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private String id;

    public ReadIndexRequest(NetSocket netSocket, NetClusterImpl netCluster) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
    }


    public void response(boolean isLeader, int readIndex){
        netCluster.readIndexResponse(netSocket,new ReadIndexResponse().setId(id).setLeader(isLeader).setReadIndex(readIndex));
    }

    public String getId() {
        return id;
    }

    public ReadIndexRequest setId(String id) {
        this.id = id;
        return this;
    }
}
