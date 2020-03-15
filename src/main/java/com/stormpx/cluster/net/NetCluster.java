package com.stormpx.cluster.net;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.InstallSnapshotMessage;
import com.stormpx.cluster.message.VoteMessage;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

import java.util.Collection;
import java.util.Set;

public interface NetCluster {

    Future<Void> init();

    NetCluster voteRequestHandler(Handler<VoteRequest> handler);

    NetCluster appendEntriesRequestHandler(Handler<AppendEntriesRequest> handler);

    NetCluster requestHandler(Handler<ClientRequest> handler);

    NetCluster readIndexRequestHandler(Handler<ReadIndexRequest> handler);

    NetCluster voteResponseHandler(Handler<VoteResponse> handler);

    NetCluster appendEntriesResponseHandler(Handler<AppendEntriesResponse> handler);

    NetCluster responseHandler(Handler<Response> handler);

    NetCluster requestIndexResponseHandler(Handler<ReadIndexResponse> handler);

    NetCluster installSnapshotRequestHandler(Handler<InstallSnapshotRequest> handler);

    NetCluster installSnapshotResponseHandler(Handler<InstallSnapshotResponse> handler);

    ClusterNode getNode(String id);

    Collection<ClusterNode> nodes();

    void initNodeIndex(int index);

    void request(String nodeId, VoteMessage voteMessage);

    void request(String nodeId, AppendEntriesMessage appendEntriesMessage);

    void request(String nodeId, InstallSnapshotMessage installSnapshotMessage);


    void request(String nodeId, Buffer payload);

    void request(Set<String> nodeIds, Buffer payload);

    void requestReadIndex(String nodeId, String id);

}
