package com.stormpx.cluster.net;

import com.stormpx.cluster.message.VoteMessage;
import io.vertx.core.net.NetSocket;

public class VoteRequest {
    private NetSocket netSocket;
    private NetClusterImpl mqttClusterServer;
    private VoteMessage voteMessage;


    public VoteRequest(NetSocket netSocket, NetClusterImpl mqttClusterServer, VoteMessage voteMessage) {
        this.netSocket = netSocket;
        this.mqttClusterServer = mqttClusterServer;
        this.voteMessage = voteMessage;
    }

    public VoteMessage getVoteMessage() {
        return voteMessage;
    }

    public void response(VoteResponse voteResponse){
        mqttClusterServer.voteResponse(netSocket,voteResponse);
    }
}
