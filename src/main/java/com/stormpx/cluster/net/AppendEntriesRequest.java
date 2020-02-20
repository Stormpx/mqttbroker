package com.stormpx.cluster.net;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.RpcMessage;
import io.vertx.core.net.NetSocket;

public class AppendEntriesRequest {

    private NetSocket netSocket;
    private NetClusterImpl clusterServer;
    private RpcMessage rpcMessage;
    private AppendEntriesMessage appendEntriesMessage;

    public AppendEntriesRequest(NetSocket netSocket, NetClusterImpl clusterServer, RpcMessage rpcMessage) {
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
