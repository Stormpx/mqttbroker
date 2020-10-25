package com.stormpx.cluster.net;

import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.ClusterMessage;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.NetSocket;

public class ClientExtendRequest {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private ClusterMessage clusterMessage;


    public ClientExtendRequest(NetSocket netSocket, NetClusterImpl netCluster, ClusterMessage clusterMessage) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
        this.clusterMessage = clusterMessage;
    }

    public ClusterMessage getClusterMessage() {
        return clusterMessage;
    }

    public void response(boolean success,int requestId, Buffer buffer){
        if (buffer==null)
            buffer=Buffer.buffer();

        ClusterMessage clusterMessage = new ClusterMessage(MessageType.RESPONSE, this.clusterMessage.getFromId(), this.clusterMessage.getTargetId(),
                Buffer.buffer().appendByte((byte) (success ? 1 : 0)).appendInt(requestId).appendBuffer(buffer));
        netCluster.tryResponse(netSocket, clusterMessage);
    }

}
