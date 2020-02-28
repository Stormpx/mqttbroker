package com.stormpx.cluster.net;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class AppendEntriesRequest {

    private NetSocket netSocket;
    private NetClusterImpl clusterServer;
    private RpcMessage rpcMessage;
    private AppendEntriesMessage appendEntriesMessage;

    public AppendEntriesRequest(NetSocket netSocket, NetClusterImpl clusterServer, RpcMessage rpcMessage) {
        this.netSocket = netSocket;
        this.clusterServer = clusterServer;
        this.rpcMessage=rpcMessage;
        this.appendEntriesMessage = AppendEntriesMessage.decode(rpcMessage.getBuffer());
    }

    public AppendEntriesMessage getAppendEntriesMessage() {
        return appendEntriesMessage;
    }



    public void response(int term,int requestLastIndex,boolean success){
        AppendEntriesResponse appendEntriesResponse = new AppendEntriesResponse().setTerm(term).setRequestLastIndex(requestLastIndex).setSuccess(success);
        RpcMessage resp = new RpcMessage(MessageType.APPENDENTRIESRESPONSE, this.rpcMessage.getFromId(), this.rpcMessage.getTargetId(), this.rpcMessage.getRequestId(),
                Json.encodeToBuffer(appendEntriesResponse));
        clusterServer.tryResponse(netSocket,resp);
    }
}
