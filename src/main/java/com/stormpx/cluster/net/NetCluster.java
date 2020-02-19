package com.stormpx.cluster.net;

import com.stormpx.cluster.ClusterNode;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.Collection;

public interface NetCluster {

    Future<Void> init();

    NetCluster voteRequestHandler(Handler<VoteRequest> handler);

    NetCluster appendEntriesRequestHandler(Handler<AppendEntriesRequest> handler);

    NetCluster rpcRequestHandler(Handler<Request> handler);

    NetCluster readIndexRequestHandler(Handler<ReadIndexRequest> handler);

    ClusterNode getNode(String id);

    Collection<ClusterNode> nodes();

    void initNodeIndex(int index);

}
