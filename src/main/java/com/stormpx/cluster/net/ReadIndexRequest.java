package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.ClusterMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class ReadIndexRequest {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private ClusterMessage clusterMessage;
    private String id;


    public ReadIndexRequest(NetSocket netSocket, NetClusterImpl netCluster, ClusterMessage clusterMessage) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
        this.clusterMessage = clusterMessage;
        this.id= clusterMessage.getBuffer().toString("utf-8");
    }


    public void response(boolean isLeader, int readIndex){
        ClusterMessage resp=new ClusterMessage(MessageType.READINDEXRESPONSE, clusterMessage.getFromId(), clusterMessage.getTargetId(),
                Json.encodeToBuffer(new ReadIndexResponse().setId(id).setLeader(isLeader).setReadIndex(readIndex)));
        netCluster.tryResponse(netSocket,resp);
    }

    public String getId() {
        return id;
    }

}
