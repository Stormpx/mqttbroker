package com.stormpx.cluster;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.VoteMessage;
import com.stormpx.cluster.net.*;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

public interface ClusterNode {

    NodeState state();

    void request(VoteMessage voteMessage);

    void request(AppendEntriesMessage appendEntriesMessage);

    void request(int requestId, Buffer buffer);

    void requestReadIndex(String id);

    ClusterNode voteResponseListener(Handler<VoteResponse> handler);

    ClusterNode appendEntriesResponseListener(Handler<AppendEntriesResponse> handler);

    ClusterNode responseListener(Handler<Response> handler);

    ClusterNode requestIndexResponseListener(Handler<ReadIndexResponse> handler);

}
