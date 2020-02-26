package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class ReadIndexRequest {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private RpcMessage rpcMessage;
    private String id;


    public ReadIndexRequest(NetSocket netSocket, NetClusterImpl netCluster, RpcMessage rpcMessage) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
        this.rpcMessage=rpcMessage;
        this.id=rpcMessage.getBuffer().toString("utf-8");
    }


    public void response(boolean isLeader, int readIndex){
        RpcMessage resp=new RpcMessage(MessageType.READINDEXRESPONSE,rpcMessage.getFromId(),rpcMessage.getTargetId(),rpcMessage.getRequestId(),
                Json.encodeToBuffer(new ReadIndexResponse().setId(id).setLeader(isLeader).setReadIndex(readIndex)));
        netCluster.tryResponse(netSocket,resp);
    }

    public String getId() {
        return id;
    }

}
