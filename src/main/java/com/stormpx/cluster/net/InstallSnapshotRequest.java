package com.stormpx.cluster.net;

import com.stormpx.cluster.message.InstallSnapshotMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import io.vertx.core.json.Json;
import io.vertx.core.net.NetSocket;

public class InstallSnapshotRequest {
    private NetSocket netSocket;
    private NetClusterImpl netCluster;
    private RpcMessage rpcMessage;
    private InstallSnapshotMessage installSnapshotMessage;

    public InstallSnapshotRequest(NetSocket netSocket, NetClusterImpl netCluster, RpcMessage rpcMessage) {
        this.netSocket = netSocket;
        this.netCluster = netCluster;
        this.rpcMessage = rpcMessage;
        this.installSnapshotMessage=InstallSnapshotMessage.decode(rpcMessage.getBuffer());

    }

    public InstallSnapshotMessage getInstallSnapshotMessage() {
        return installSnapshotMessage;
    }

    public void response(boolean accept,int lastNum,int term){
        InstallSnapshotResponse installSnapshotResponse = new InstallSnapshotResponse().setAccept(accept).setLastNum(lastNum).setTerm(term);
        RpcMessage rpcMessage = new RpcMessage(MessageType.INSTALLSNAPSHOTRESPONSE, this.rpcMessage.getFromId(), this.rpcMessage.getTargetId(), 0,
                Json.encodeToBuffer(installSnapshotResponse));
        netCluster.tryResponse(netSocket,rpcMessage);
    }

}
