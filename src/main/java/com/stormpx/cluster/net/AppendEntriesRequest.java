package com.stormpx.cluster.net;

import com.stormpx.cluster.message.AppendEntriesMessage;
import io.vertx.core.net.NetSocket;

public class AppendEntriesRequest {

    private NetSocket netSocket;
    private NetClusterImpl clusterServer;
    private AppendEntriesMessage appendEntriesMessage;

    public AppendEntriesRequest(NetSocket netSocket, NetClusterImpl clusterServer, AppendEntriesMessage appendEntriesMessage) {
        this.netSocket = netSocket;
        this.clusterServer = clusterServer;
        this.appendEntriesMessage = appendEntriesMessage;
    }

    public AppendEntriesMessage getAppendEntriesMessage() {
        return appendEntriesMessage;
    }

    public AppendEntriesRequest setAppendEntriesMessage(AppendEntriesMessage appendEntriesMessage) {
        this.appendEntriesMessage = appendEntriesMessage;
        return this;
    }


    public void response(int term,int requestLastIndex,boolean success){
        AppendEntriesResponse appendEntriesResponse = new AppendEntriesResponse().setTerm(term).setRequestLastIndex(requestLastIndex).setSuccess(success);
        clusterServer.appendEntriesResponse(netSocket,appendEntriesResponse);
    }
}
