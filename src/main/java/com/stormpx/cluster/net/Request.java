package com.stormpx.cluster.net;

import com.stormpx.cluster.message.RpcMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

public class Request {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private RpcMessage rpcMessage;


    public Request(NetSocket netSocket, NetClusterImpl netCluster, RpcMessage rpcMessage) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
        this.rpcMessage = rpcMessage;
    }

    public RpcMessage getRpcMessage() {
        return rpcMessage;
    }

    public void response(boolean success, Buffer buffer){
        netCluster.rpcResponse(netSocket,rpcMessage.getRequestId(),success,buffer);
    }

}
