package com.stormpx.cluster.net;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.NodeState;
import com.stormpx.cluster.message.*;
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
    private final static Logger logger= LoggerFactory.getLogger("net");
    private Vertx vertx;
    private JsonObject config;
    private String id;
    private NetServer netServer;
    private Set<NetSocket> netSockets;
    private Handler<VoteRequest> voteRequestHandler;
    private Handler<AppendEntriesRequest> appendEntriesRequestHandler;
    private Handler<Request> clusterRequestHandler;
    private Handler<ReadIndexRequest> readIndexRequestHandler;
    private Handler<VoteResponse> voteResponseHandler;
    private Handler<AppendEntriesResponse> appendEntriesResponseHandler;
    private Handler<Response> responseHandler;
    private Handler<ReadIndexResponse> readIndexResponseHandler;
    private Handler<InstallSnapshotRequest> installSnapshotRequestHandler;
    private Handler<InstallSnapshotResponse> installSnapshotResponseHandler;
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
        this.id=config.getString("id");
        Promise<NetServer> promise=Promise.promise();
        this.netServer.connectHandler(netSocket->{

            logger.debug("new socket address: {}",netSocket.remoteAddress());
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
                ClusterNodeImpl mqttClusterNodeImpl = new ClusterNodeImpl(vertx,e.getKey(), socketAddress);
                mqttClusterNodeImpl.messageHandler(msg->callHandler(null,msg));
                nodeMap.put(mqttClusterNodeImpl.getNodeId(), mqttClusterNodeImpl);
                mqttClusterNodeImpl.connect();
            });
        } catch (Exception e) {
            e.printStackTrace();
            return Future.failedFuture(e);
        }
        return Future.succeededFuture();
    }


    private void callHandler(NetSocket netSocket, RpcMessage rpcMessage){
        String targetId = rpcMessage.getTargetId();
        if (!targetId.equals(id)){
            proxy(rpcMessage);
            return;
        }
        if (logger.isDebugEnabled())
            logger.debug("receive messageType:{} targetId:{} fromId:{} requestId:{} payload: {}",
                    rpcMessage.getMessageType(),rpcMessage.getTargetId(),rpcMessage.getFromId(),rpcMessage.getRequestId(),rpcMessage.getBuffer());

        switch (rpcMessage.getMessageType()){

            case APPENDENTRIESREQUEST:
                AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(netSocket, this, rpcMessage);
                Handler<AppendEntriesRequest> handler = this.appendEntriesRequestHandler;
                if (handler!=null)
                    handler.handle(appendEntriesRequest);
                break;
            case VOTEREQUEST:
                VoteRequest voteRequest = new VoteRequest(netSocket, this, rpcMessage);
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
            case READINDEXREQUEST:
                ReadIndexRequest readIndexRequest = new ReadIndexRequest(netSocket, this,rpcMessage);
                Handler<ReadIndexRequest> readIndexRequestHandler = this.readIndexRequestHandler;
                if (readIndexRequestHandler!=null)
                    readIndexRequestHandler.handle(readIndexRequest);
                break;

            case APPENDENTRIESRESPONSE:
                Handler<AppendEntriesResponse> appendEntriesResponseHandler = this.appendEntriesResponseHandler;
                if (appendEntriesResponseHandler!=null)
                    appendEntriesResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),AppendEntriesResponse.class).setNodeId(rpcMessage.getFromId()));
                break;
            case VOTERESPONSE:
                Handler<VoteResponse> voteResponseHandler = this.voteResponseHandler;
                if (voteResponseHandler!=null)
                    voteResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),VoteResponse.class).setNodeId(rpcMessage.getFromId()));
                break;
            case RESPONSE:
                Buffer buffer = rpcMessage.getBuffer();
                Buffer payload = buffer.slice(1, buffer.length());
                Response response = new Response().setSuccess(buffer.getByte(0) == 1).setPayload(payload).setRequestId(rpcMessage.getRequestId()).setNodeId(rpcMessage.getFromId());
                Handler<Response> responseHandler = this.responseHandler;
                if (responseHandler!=null)
                    responseHandler.handle(response);
                break;
            case READINDEXRESPONSE:
                Handler<ReadIndexResponse> readIndexResponseHandler = this.readIndexResponseHandler;
                if (readIndexResponseHandler!=null)
                    readIndexResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),ReadIndexResponse.class));
                break;
            case INSTALLSNAPSHOTREQUEST:
                Handler<InstallSnapshotRequest> installSnapshotRequestHandler = this.installSnapshotRequestHandler;
                if (installSnapshotRequestHandler!=null)
                    installSnapshotRequestHandler.handle(new InstallSnapshotRequest(netSocket,this,rpcMessage));
                break;
            case INSTALLSNAPSHOTRESPONSE:
                Handler<InstallSnapshotResponse> installSnapshotResponseHandler = this.installSnapshotResponseHandler;
                if (installSnapshotResponseHandler!=null)
                    installSnapshotResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),InstallSnapshotResponse.class));
                break;
        }
    }

    private void proxy(RpcMessage rpcMessage) {
        String targetId = rpcMessage.getTargetId();
        ClusterNode clusterNode = nodeMap.get(targetId);
        if (clusterNode!=null){
            clusterNode.send(rpcMessage.encode());
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
    public NetCluster requestHandler(Handler<Request> handler) {
        this.clusterRequestHandler=handler;
        return this;
    }

    @Override
    public NetCluster readIndexRequestHandler(Handler<ReadIndexRequest> handler) {
        this.readIndexRequestHandler=handler;
        return this;
    }

    @Override
    public NetCluster voteResponseHandler(Handler<VoteResponse> handler) {
        this.voteResponseHandler=handler;
        return this;
    }

    @Override
    public NetCluster appendEntriesResponseHandler(Handler<AppendEntriesResponse> handler) {
        this.appendEntriesResponseHandler=handler;
        return this;
    }

    @Override
    public NetCluster responseHandler(Handler<Response> handler) {
        this.responseHandler=handler;
        return this;
    }

    @Override
    public NetCluster requestIndexResponseHandler(Handler<ReadIndexResponse> handler) {
        this.readIndexResponseHandler=handler;
        return this;
    }

    @Override
    public NetCluster installSnapshotRequestHandler(Handler<InstallSnapshotRequest> handler) {
        this.installSnapshotRequestHandler=handler;
        return this;
    }

    @Override
    public NetCluster installSnapshotResponseHandler(Handler<InstallSnapshotResponse> handler) {
        this.installSnapshotResponseHandler=handler;
        return this;
    }

    void tryResponse(NetSocket netSocket,RpcMessage rpcMessage){

        if (!netSockets.contains(netSocket)){
            // other way
            sendOrProxy(rpcMessage);
            return;
        }
        logRpcMessage(rpcMessage);

        netSocket.write(rpcMessage.encode());
    }

    private void sendOrProxy(RpcMessage rpcMessage){

        ClusterNode clusterNode = nodeMap.get(rpcMessage.getTargetId());
        if (clusterNode.isActive()){
           logRpcMessage(rpcMessage);
            clusterNode.send(rpcMessage.encode());
        }else{
            nodeMap.values().stream().filter(ClusterNode::isActive).findAny()
                    .ifPresent(cn-> {
                        logRpcMessage(rpcMessage);
                        cn.send(rpcMessage.encode());
                    });
        }
    }

    private void logRpcMessage(RpcMessage rpcMessage){
        if (logger.isDebugEnabled())
            logger.debug("send messageType:{} targetId:{} fromId:{} requestId:{} payload: {}",
                    rpcMessage.getMessageType(),rpcMessage.getTargetId(),rpcMessage.getFromId(),rpcMessage.getRequestId(),rpcMessage.getBuffer());
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

    @Override
    public void request(String nodeId, VoteMessage voteMessage) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        RpcMessage rpcMessage = new RpcMessage(MessageType.VOTEREQUEST, nodeId, id, 0, Json.encodeToBuffer(voteMessage));
        sendOrProxy(rpcMessage);
    }

    @Override
    public void request(String nodeId, AppendEntriesMessage appendEntriesMessage) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }


        RpcMessage rpcMessage = new RpcMessage(MessageType.APPENDENTRIESREQUEST, nodeId, id, 0, appendEntriesMessage.encode());
        sendOrProxy(rpcMessage);
    }

    @Override
    public void request(String nodeId, InstallSnapshotMessage installSnapshotMessage) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        RpcMessage rpcMessage = new RpcMessage(MessageType.REQUEST, nodeId, id, 0, installSnapshotMessage.encode());
        sendOrProxy(rpcMessage);
    }

    @Override
    public void request(String nodeId, int requestId, Buffer payload) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        RpcMessage rpcMessage = new RpcMessage(MessageType.REQUEST, nodeId, id, requestId, payload);
        sendOrProxy(rpcMessage);
    }

    @Override
    public void request(Set<String> nodeIds, int requestId, Buffer payload) {
        nodeIds.forEach(targetId->{
            request(targetId,requestId,payload);
        });
    }

    @Override
    public void requestReadIndex(String nodeId, String id) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        RpcMessage rpcMessage = new RpcMessage(MessageType.READINDEXREQUEST, nodeId, this.id, 0, Buffer.buffer(id, "utf-8"));
        sendOrProxy(rpcMessage);
    }

    private boolean nodeEexist(String id){
        return nodeMap.containsKey(id);
    }


}
