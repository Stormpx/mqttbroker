package com.stormpx.cluster.net;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.NodeState;
import com.stormpx.cluster.message.AppendEntriesMessage;
import com.stormpx.cluster.message.MessageType;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.VoteMessage;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

public class ClusterClient implements ClusterNode {
    private final static Logger logger= LoggerFactory.getLogger("mqttbroker");
    private Vertx vertx;
    private String id;
    private NetClient netClient;
    private SocketAddress socketAddress;
    private NetSocket netSocket;
    private boolean active;
    private NodeState nodeState;

    private Handler<VoteResponse> voteResponseHandler;
    private Handler<AppendEntriesResponse> appendEntriesResponseHandler;
    private Handler<Response> responseHandler;
    private Handler<ReadIndexResponse> readIndexResponseHandler;


    public ClusterClient(Vertx vertx, String id, SocketAddress socketAddress) {
        this.vertx=vertx;
        this.id=id;
        this.netClient = vertx.createNetClient(new NetClientOptions().setReconnectAttempts(0));
        this.socketAddress = socketAddress;
        this.nodeState=new NodeState();
    }

    private void setHandler(NetSocket netSocket){
        active=true;
        SocketHandler socketHandler = new SocketHandler();
        socketHandler.messageHandler(this::callHandler);
        netSocket.exceptionHandler(t->logger.error("",t));
        netSocket.handler(socketHandler);
        netSocket.closeHandler(v->{
            logger.info("node:{} socket close try reconnect ",socketAddress);
            active=false;
            connect();
        });
    }

    private void callHandler(RpcMessage rpcMessage){
        switch (rpcMessage.getMessageType()){

            case APPENDENTRIES:
                Handler<AppendEntriesResponse> appendEntriesResponseHandler = this.appendEntriesResponseHandler;
                if (appendEntriesResponseHandler!=null)
                    appendEntriesResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),AppendEntriesResponse.class).setNodeId(id));
                break;
            case VOTE:
                Handler<VoteResponse> voteResponseHandler = this.voteResponseHandler;
                if (voteResponseHandler!=null)
                    voteResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),VoteResponse.class).setNodeId(id));
                break;
            case REQUEST:
                Buffer buffer = rpcMessage.getBuffer();
                Buffer payload = buffer.slice(1, buffer.length());
                Response response = new Response().setSuccess(buffer.getByte(0) == 1).setPayload(payload).setRequestId(rpcMessage.getRequestId());
                Handler<Response> responseHandler = this.responseHandler;
                if (responseHandler!=null)
                    responseHandler.handle(response);
                break;
            case READINDEX:
                Handler<ReadIndexResponse> readIndexResponseHandler = this.readIndexResponseHandler;
                if (readIndexResponseHandler!=null)
                    readIndexResponseHandler.handle(Json.decodeValue(rpcMessage.getBuffer(),ReadIndexResponse.class));
                break;
        }
    }

    public Future<ClusterClient> connect(){
        return connect0().onFailure(t->{
            logger.info("connect node: {} fail cause:{} try again ",id,t.getMessage());
            //                    reConnect();
            vertx.setTimer(10*1000,id->connect());
        });

    }
    public Future<ClusterClient> connect0(){

        Promise<ClusterClient> promise = Promise.promise();
        netClient.connect(socketAddress, ar->{
            if (ar.succeeded()){
                NetSocket netSocket = ar.result();
                setHandler(netSocket);
                this.netSocket=netSocket;
                promise.complete(this);
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }


    @Override
    public NodeState state() {
        return nodeState;
    }

    public void request(VoteMessage voteMessage){
        if (!active)
            return;
//        System.out.println("ready to send");
        Buffer buffer = Json.encodeToBuffer(voteMessage);
        tryWrite(MessageType.VOTE,buffer);

    }

    @Override
    public void request(AppendEntriesMessage appendEntriesMessage) {
        if (!active)
            return;
        Buffer buffer = Json.encodeToBuffer(appendEntriesMessage);
        tryWrite(MessageType.APPENDENTRIES,buffer);
    }

    @Override
    public void request(int requestId, Buffer buffer) {
        if (!active) {
            return;
        }
        netSocket.write(Buffer.buffer(9+buffer.length()).appendByte((byte) MessageType.REQUEST.getValue())
                .appendInt(requestId)
                .appendInt(buffer.length())
                .appendBuffer(buffer));

    }

    @Override
    public void requestReadIndex(String id) {
        if (!active) {
            return;
        }
        tryWrite(MessageType.APPENDENTRIES,Buffer.buffer(id,"utf-8"));

    }


    private void tryWrite(MessageType messageType,Buffer buffer){
        netSocket.write(Buffer.buffer(5+buffer.length()).appendByte((byte) messageType.getValue()).appendInt(buffer.length()).appendBuffer(buffer));
    }

    @Override
    public ClusterNode voteResponseListener(Handler<VoteResponse> handler) {
        this.voteResponseHandler=handler;
        return this;
    }

    @Override
    public ClusterNode appendEntriesResponseListener(Handler<AppendEntriesResponse> handler) {
        this.appendEntriesResponseHandler=handler;
        return this;
    }

    @Override
    public ClusterNode responseListener(Handler<Response> handler) {
        this.responseHandler=handler;
        return this;
    }

    @Override
    public ClusterNode requestIndexResponseListener(Handler<ReadIndexResponse> handler) {
        this.readIndexResponseHandler=handler;
        return this;
    }


    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public boolean isActive() {
        return active;
    }


    public String getId() {
        return id;
    }
}
