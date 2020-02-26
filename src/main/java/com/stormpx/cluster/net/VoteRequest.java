package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.VoteMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class VoteRequest {
    private NetSocket netSocket;
    private NetClusterImpl mqttClusterServer;
    private RpcMessage rpcMessage;
    private VoteMessage voteMessage;


    public VoteRequest(NetSocket netSocket, NetClusterImpl mqttClusterServer,RpcMessage rpcMessage) {
        this.netSocket = netSocket;
        this.mqttClusterServer = mqttClusterServer;
        this.rpcMessage=rpcMessage;
        this.voteMessage = Json.decodeValue(rpcMessage.getBuffer(), VoteMessage.class);
    }

    public VoteMessage getVoteMessage() {
        return voteMessage;
    }

    public void response(VoteResponse voteResponse){
        mqttClusterServer.tryResponse(netSocket,new RpcMessage(MessageType.VOTERESPONSE,rpcMessage.getFromId(),rpcMessage.getTargetId(),0, Json.encodeToBuffer(voteResponse)));
    }
}
