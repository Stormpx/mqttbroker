package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.ClusterMessage;
import com.stormpx.cluster.message.VoteMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class VoteRequest {
    private NetSocket netSocket;
    private NetClusterImpl mqttClusterServer;
    private ClusterMessage clusterMessage;
    private VoteMessage voteMessage;


    public VoteRequest(NetSocket netSocket, NetClusterImpl mqttClusterServer, ClusterMessage clusterMessage) {
        this.netSocket = netSocket;
        this.mqttClusterServer = mqttClusterServer;
        this.clusterMessage = clusterMessage;
        this.voteMessage = Json.decodeValue(clusterMessage.getBuffer(), VoteMessage.class);
    }

    public VoteMessage getVoteMessage() {
        return voteMessage;
    }

    public void response(VoteResponse voteResponse){
        mqttClusterServer.tryResponse(netSocket,new ClusterMessage(MessageType.VOTERESPONSE, clusterMessage.getFromId(), clusterMessage.getTargetId(), Json.encodeToBuffer(voteResponse)));
    }
}
