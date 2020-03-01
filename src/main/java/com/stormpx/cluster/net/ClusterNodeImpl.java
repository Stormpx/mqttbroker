package com.stormpx.cluster.net;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.NodeState;
import com.stormpx.cluster.message.RpcMessage;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetClient;
import io.vertx.core.net.NetClientOptions;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

public class ClusterNodeImpl implements ClusterNode {
    private final static Logger logger= LoggerFactory.getLogger("mqttbroker");
    private Vertx vertx;
    private String nodeId;
    private NetClient netClient;
    private SocketAddress socketAddress;
    private NetSocket netSocket;
    private boolean active;
    private NodeState nodeState;

    private Handler<RpcMessage> messageHandler;


    public ClusterNodeImpl(Vertx vertx, String id, SocketAddress socketAddress) {
        this.vertx=vertx;
        this.nodeId =id;
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
        Handler<RpcMessage> messageHandler = this.messageHandler;
        if (messageHandler!=null)
            messageHandler.handle(rpcMessage);

    }

    public Future<ClusterNodeImpl> connect(){
        return connect0().onFailure(t->{
            logger.warn("connect node: {} fail cause:{} try again ", nodeId,t.getMessage());
            //                    reConnect();
            vertx.setTimer(5*1000,id->connect());
        });

    }
    public Future<ClusterNodeImpl> connect0(){

        Promise<ClusterNodeImpl> promise = Promise.promise();
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
    public String id() {
        return nodeId;
    }

    @Override
    public NodeState state() {
        return nodeState;
    }
    @Override
    public boolean isActive(){
        return active;
    }

    @Override
    public void send(Buffer buffer) {
        if (!active) {
            return;
        }
        netSocket.write(buffer);
    }

    @Override
    public ClusterNode messageHandler(Handler<RpcMessage> handler) {
        this.messageHandler=handler;
        return this;
    }


    public SocketAddress getSocketAddress() {
        return socketAddress;
    }


    public String getNodeId() {
        return nodeId;
    }
}
