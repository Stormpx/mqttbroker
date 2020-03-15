package com.stormpx.cluster.net;

import com.stormpx.cluster.message.InstallSnapshotMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.ClusterMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class InstallSnapshotRequest {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private ClusterMessage clusterMessage;
    private InstallSnapshotMessage installSnapshotMessage;

    public InstallSnapshotRequest(NetSocket netSocket, NetClusterImpl netCluster, ClusterMessage clusterMessage) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
        this.clusterMessage = clusterMessage;
        this.installSnapshotMessage=InstallSnapshotMessage.decode(clusterMessage.getBuffer());

    }

    public InstallSnapshotMessage getInstallSnapshotMessage() {
        return installSnapshotMessage;
    }

    public void response(boolean accept,boolean done,int offset,int term){
        InstallSnapshotResponse installSnapshotResponse = new InstallSnapshotResponse().setNodeId(this.clusterMessage.getTargetId()).setAccept(accept).setDone(done).setNextOffset(offset).setTerm(term);
        ClusterMessage clusterMessage = new ClusterMessage(MessageType.INSTALLSNAPSHOTRESPONSE, this.clusterMessage.getFromId(), this.clusterMessage.getTargetId(),
                Json.encodeToBuffer(installSnapshotResponse));
        netCluster.tryResponse(netSocket, clusterMessage);
    }

}
