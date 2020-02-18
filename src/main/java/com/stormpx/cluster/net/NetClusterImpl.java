package com.stormpx.cluster.net;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.NodeState;
import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.VoteMessage;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetServer;
import io.vertx.core.net.NetServerOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

import java.util.*;

public class NetClusterImpl implements NetCluster {
    private final static Logger logger= LoggerFactory.getLogger(NetCluster.class);
    private Vertx vertx;
    private JsonObject config;
    private NetServer netServer;
    private Set<NetSocket> netSockets;
    private Handler<VoteRequest> voteRequestHandler;
    private Handler<AppendEntriesRequest> appendEntriesRequestHandler;
    private Handler<Request> clusterRequestHandler;
    //key nodeId value client
    private Map<String, ClusterNode> nodeMap;

    public NetClusterImpl(Vertx vertx, JsonObject config) {
        this.vertx = vertx;
        this.config = config;
        this.nodeMap =new HashMap<>();
    }

    @Override
    public Future<Void> init() {
        this.netSockets=new HashSet<>();
        this.netServer=vertx.createNetServer(new NetServerOptions().setPort(config.getInteger("port")));
        Promise<NetServer> promise=Promise.promise();
        this.netServer.connectHandler(netSocket->{
            System.out.println("new socket");
            netSockets.add(netSocket);
            SocketHandler socketHandler = new SocketHandler();
            socketHandler.messageHandler(msg-> callHandler(netSocket,msg));
            netSocket.exceptionHandler(Throwable::printStackTrace);
            netSocket.handler(socketHandler);
            netSocket.endHandler(v->netSockets.remove(netSocket));
        });
        this.netServer.listen(promise);
        return promise.future().map((Void)null)
                .onSuccess(v->{
                    initConnect();
                });
    }

    private Future<Void> initConnect(){


        JsonObject nodes =
                config.getJsonObject("nodes");

        try {
            nodes.forEach(e -> {
                String address = e.getValue().toString();
                String[] addresss = address.split(":");
                SocketAddress socketAddress = SocketAddress.inetSocketAddress(Integer.parseInt(addresss[1]), addresss[0]);
                logger.info("start connect node: {}",e.getKey());
                ClusterClient mqttClusterClient = new ClusterClient(vertx,e.getKey(), socketAddress);
                nodeMap.put(mqttClusterClient.getId(),mqttClusterClient);
                mqttClusterClient.connect();
            });
        } catch (Exception e) {
            e.printStackTrace();
            return Future.failedFuture(e);
        }
        return Future.succeededFuture();
    }


    private void callHandler(NetSocket netSocket, RpcMessage rpcMessage){
        switch (rpcMessage.getMessageType()){

            case APPENDENTRIES:
                AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(netSocket, this, Json.decodeValue(rpcMessage.getBuffer(), AppendEntriesMessage.class));
                Handler<AppendEntriesRequest> handler = this.appendEntriesRequestHandler;
                if (handler!=null)
                    handler.handle(appendEntriesRequest);
                break;
            case VOTE:
                VoteRequest voteRequest = new VoteRequest(netSocket, this, Json.decodeValue(rpcMessage.getBuffer(), VoteMessage.class));
                Handler<VoteRequest> voteRequestHandler = this.voteRequestHandler;
                if (voteRequestHandler!=null)
                    voteRequestHandler.handle(voteRequest);
                break;
            case REQUEST:
                Request request = new Request(netSocket, this, rpcMessage);
                Handler<Request> clusterRequestHandler = this.clusterRequestHandler;
                if (clusterRequestHandler!=null)
                    clusterRequestHandler.handle(request);
                break;
        }
    }
    @Override
    public NetCluster voteRequestHandler(Handler<VoteRequest> handler) {
        this.voteRequestHandler =handler;
        return this;
    }

    @Override
    public NetCluster appendEntriesRequestHandler(Handler<AppendEntriesRequest> handler) {
        this.appendEntriesRequestHandler=handler;
        return this;
    }

    @Override
    public NetCluster rpcRequestHandler(Handler<Request> handler) {
        this.clusterRequestHandler=handler;
        return this;
    }


    void voteResponse(NetSocket netSocket, VoteResponse voteResponse) {
        //check active
        if (!netSockets.contains(netSocket))
            return ;

        Buffer buffer = Json.encodeToBuffer(voteResponse);

        netSocket.write(Buffer.buffer(5+buffer.length()).appendByte((byte) MessageType.VOTE.getValue()).appendInt(buffer.length()).appendBuffer(buffer));
    }

    void appendEntriesResponse(NetSocket netSocket, AppendEntriesResponse appendEntriesResponse) {
        //check active
        if (!netSockets.contains(netSocket))
            return ;
        Buffer buffer = Json.encodeToBuffer(appendEntriesResponse);
        netSocket.write(Buffer.buffer(5+buffer.length()).appendByte((byte) MessageType.APPENDENTRIES.getValue()).appendInt(buffer.length()).appendBuffer(buffer));
    }

    void rpcResponse(NetSocket netSocket,int requestId,boolean success,Buffer buffer){
        if (!netSockets.contains(netSocket))
            return ;

        ByteBuf byteBuf = Unpooled.buffer(9)
                .writeByte(MessageType.REQUEST.getValue())
                .writeInt(requestId)
                .writeInt(buffer.length()+1)
                .writeByte(success?1:0);
        netSocket.write(Buffer.buffer(Unpooled.compositeBuffer()
                .addComponent(true,byteBuf)
                .addComponent(true,buffer.getByteBuf())));

    }


    @Override
    public ClusterNode getNode(String id) {
        return nodeMap.get(id);
    }

    @Override
    public Collection<ClusterNode> nodes() {
        return nodeMap.values();
    }

    @Override
    public void initNodeIndex(int index) {
        nodeMap.values().forEach(clusterNode -> {
            NodeState nodeState = clusterNode.state();
            nodeState.setNextIndex(index);
            nodeState.setMatchIndex(0);
        });
    }


}
