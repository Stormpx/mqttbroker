package com.stormpx.cluster;

import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.VoteMessage;
import com.stormpx.cluster.net.*;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

public interface ClusterNode {

    String id();

    NodeState state();

 /*   void request(String nodeId,VoteMessage voteMessage);

    void request(String nodeId,AppendEntriesMessage appendEntriesMessage);

    void request(String nodeId,int requestId, Buffer buffer);

    void requestReadIndex(String nodeId,String id);*/

    boolean isActive();

    void send(Buffer buffer);

    ClusterNode messageHandler(Handler<RpcMessage> handler);


}
