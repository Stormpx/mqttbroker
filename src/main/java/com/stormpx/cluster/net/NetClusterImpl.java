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
    private Handler<ClientRequest> clusterRequestHandler;
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


    private void callHandler(NetSocket netSocket, ClusterMessage clusterMessage){
        String targetId = clusterMessage.getTargetId();
        if (!targetId.equals(id)){
            proxy(clusterMessage);
            return;
        }
        if (logger.isDebugEnabled())
            logger.debug("receive messageType:{} targetId:{} fromId:{} payload: {}",
                    clusterMessage.getMessageType(), clusterMessage.getTargetId(), clusterMessage.getFromId(),clusterMessage.getBuffer());

        switch (clusterMessage.getMessageType()){

            case APPENDENTRIESREQUEST:
                AppendEntriesRequest appendEntriesRequest = new AppendEntriesRequest(netSocket, this, clusterMessage);
                Handler<AppendEntriesRequest> handler = this.appendEntriesRequestHandler;
                if (handler!=null)
                    handler.handle(appendEntriesRequest);
                break;
            case VOTEREQUEST:
                VoteRequest voteRequest = new VoteRequest(netSocket, this, clusterMessage);
                Handler<VoteRequest> voteRequestHandler = this.voteRequestHandler;
                if (voteRequestHandler!=null)
                    voteRequestHandler.handle(voteRequest);
                break;
            case REQUEST:
                ClientRequest request = new ClientRequest(netSocket, this, clusterMessage);
                Handler<ClientRequest> clusterRequestHandler = this.clusterRequestHandler;
                if (clusterRequestHandler!=null)
                    clusterRequestHandler.handle(request);
                break;
            case READINDEXREQUEST:
                ReadIndexRequest readIndexRequest = new ReadIndexRequest(netSocket, this, clusterMessage);
                Handler<ReadIndexRequest> readIndexRequestHandler = this.readIndexRequestHandler;
                if (readIndexRequestHandler!=null)
                    readIndexRequestHandler.handle(readIndexRequest);
                break;

            case APPENDENTRIESRESPONSE:
                Handler<AppendEntriesResponse> appendEntriesResponseHandler = this.appendEntriesResponseHandler;
                if (appendEntriesResponseHandler!=null)
                    appendEntriesResponseHandler.handle(Json.decodeValue(clusterMessage.getBuffer(),AppendEntriesResponse.class).setNodeId(clusterMessage.getFromId()));
                break;
            case VOTERESPONSE:
                Handler<VoteResponse> voteResponseHandler = this.voteResponseHandler;
                if (voteResponseHandler!=null)
                    voteResponseHandler.handle(Json.decodeValue(clusterMessage.getBuffer(),VoteResponse.class).setNodeId(clusterMessage.getFromId()));
                break;
            case RESPONSE:
                Buffer buffer = clusterMessage.getBuffer();
                int requestId = buffer.getInt(1);
                Buffer payload = buffer.slice(5, buffer.length());
                Response response = new Response().setSuccess(buffer.getByte(0) == 1).setPayload(payload).setRequestId(requestId).setNodeId(clusterMessage.getFromId());
                Handler<Response> responseHandler = this.responseHandler;
                if (responseHandler!=null)
                    responseHandler.handle(response);
                break;
            case READINDEXRESPONSE:
                Handler<ReadIndexResponse> readIndexResponseHandler = this.readIndexResponseHandler;
                if (readIndexResponseHandler!=null)
                    readIndexResponseHandler.handle(Json.decodeValue(clusterMessage.getBuffer(),ReadIndexResponse.class));
                break;
            case INSTALLSNAPSHOTREQUEST:
                Handler<InstallSnapshotRequest> installSnapshotRequestHandler = this.installSnapshotRequestHandler;
                if (installSnapshotRequestHandler!=null)
                    installSnapshotRequestHandler.handle(new InstallSnapshotRequest(netSocket,this, clusterMessage));
                break;
            case INSTALLSNAPSHOTRESPONSE:
                Handler<InstallSnapshotResponse> installSnapshotResponseHandler = this.installSnapshotResponseHandler;
                if (installSnapshotResponseHandler!=null)
                    installSnapshotResponseHandler.handle(Json.decodeValue(clusterMessage.getBuffer(),InstallSnapshotResponse.class));
                break;
        }
    }

    private void proxy(ClusterMessage clusterMessage) {
        String targetId = clusterMessage.getTargetId();
        ClusterNode clusterNode = nodeMap.get(targetId);
        if (clusterNode!=null){
            clusterNode.send(clusterMessage.encode());
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
    public NetCluster requestHandler(Handler<ClientRequest> handler) {
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

    void tryResponse(NetSocket netSocket, ClusterMessage clusterMessage){

        if (!netSockets.contains(netSocket)){
            // other way
            sendOrProxy(clusterMessage);
            return;
        }
        logClusterMessage(clusterMessage);

        netSocket.write(clusterMessage.encode());
    }

    private void sendOrProxy(ClusterMessage clusterMessage){

        ClusterNode clusterNode = nodeMap.get(clusterMessage.getTargetId());
        if (clusterNode.isActive()){
           logClusterMessage(clusterMessage);
            clusterNode.send(clusterMessage.encode());
        }else{
            nodeMap.values().stream().filter(ClusterNode::isActive).findAny()
                    .ifPresent(cn-> {
                        logClusterMessage(clusterMessage);
                        cn.send(clusterMessage.encode());
                    });
        }
    }

    private void logClusterMessage(ClusterMessage clusterMessage){
        if (logger.isDebugEnabled())
            logger.debug("send messageType:{} targetId:{} fromId:{}  payload:{}",
                    clusterMessage.getMessageType(), clusterMessage.getTargetId(), clusterMessage.getFromId(), clusterMessage.getBuffer());
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
        ClusterMessage clusterMessage = new ClusterMessage(MessageType.VOTEREQUEST, nodeId, id,  Json.encodeToBuffer(voteMessage));
        sendOrProxy(clusterMessage);
    }

    @Override
    public void request(String nodeId, AppendEntriesMessage appendEntriesMessage) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }


        ClusterMessage clusterMessage = new ClusterMessage(MessageType.APPENDENTRIESREQUEST, nodeId, id,  appendEntriesMessage.encode());
        sendOrProxy(clusterMessage);
    }

    @Override
    public void request(String nodeId, InstallSnapshotMessage installSnapshotMessage) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        ClusterMessage clusterMessage = new ClusterMessage(MessageType.INSTALLSNAPSHOTREQUEST, nodeId, id,  installSnapshotMessage.encode());
        sendOrProxy(clusterMessage);
    }

    @Override
    public void request(String nodeId, Buffer payload) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        ClusterMessage clusterMessage = new ClusterMessage(MessageType.REQUEST, nodeId, id,  payload);
        sendOrProxy(clusterMessage);
    }

    @Override
    public void request(Set<String> nodeIds, Buffer payload) {
        nodeIds.forEach(targetId->{
            request(targetId,payload);
        });
    }

    @Override
    public void requestReadIndex(String nodeId, String id) {
        if (!nodeEexist(nodeId)) {
            logger.error("unknown node: {}",nodeId);
            return;
        }
        ClusterMessage clusterMessage = new ClusterMessage(MessageType.READINDEXREQUEST, nodeId, this.id,  Buffer.buffer(id, "utf-8"));
        sendOrProxy(clusterMessage);
    }

    private boolean nodeEexist(String id){
        return nodeMap.containsKey(id);
    }


}
