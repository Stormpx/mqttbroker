package com.stormpx.cluster.net;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.ClusterMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class AppendEntriesRequest {

    private NetSocket netSocket;
    private NetClusterImpl clusterServer;
    private ClusterMessage clusterMessage;
    private AppendEntriesMessage appendEntriesMessage;

    public AppendEntriesRequest(NetSocket netSocket, NetClusterImpl clusterServer, ClusterMessage clusterMessage) {
        this.netSocket = netSocket;
        this.clusterServer = clusterServer;
        this.clusterMessage = clusterMessage;
        this.appendEntriesMessage = AppendEntriesMessage.decode(clusterMessage.getBuffer());
    }

    public AppendEntriesMessage getAppendEntriesMessage() {
        return appendEntriesMessage;
    }



    public void response(int term,int requestLastIndex,boolean success){
        AppendEntriesResponse appendEntriesResponse = new AppendEntriesResponse().setTerm(term).setRequestLastIndex(requestLastIndex).setSuccess(success);
        ClusterMessage resp = new ClusterMessage(MessageType.APPENDENTRIESRESPONSE, this.clusterMessage.getFromId(), this.clusterMessage.getTargetId(),
                Json.encodeToBuffer(appendEntriesResponse));
        clusterServer.tryResponse(netSocket,resp);
    }
}
