package com.stormpx.cluster;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.VoteMessage;
import com.stormpx.cluster.net.AppendEntriesResponse;
import com.stormpx.cluster.net.Response;
import com.stormpx.cluster.net.VoteResponse;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

public interface ClusterNode {

    NodeState state();

    void request(VoteMessage voteMessage);

    void request(AppendEntriesMessage appendEntriesMessage);

    void request(int requestId, Buffer buffer);

    ClusterNode voteResponseListener(Handler<VoteResponse> handler);

    ClusterNode appendEntriesResponseListener(Handler<AppendEntriesResponse> handler);

    ClusterNode responseListener(Handler<Response> handler);

}
