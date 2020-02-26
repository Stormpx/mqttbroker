package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
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
        RpcMessage rpcMessage = new RpcMessage(MessageType.RESPONSE, this.rpcMessage.getFromId(), this.rpcMessage.getTargetId(), this.rpcMessage.getRequestId(), Buffer.buffer().appendByte((byte) (success ? 1 : 0)).appendBuffer(buffer));
        netCluster.tryResponse(netSocket,rpcMessage);
    }

}
