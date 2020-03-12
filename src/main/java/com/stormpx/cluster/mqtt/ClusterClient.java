package com.stormpx.cluster.mqtt;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.message.*;
import com.stormpx.cluster.net.NetCluster;
import com.stormpx.cluster.net.Response;
import com.stormpx.store.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClusterClient {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);
    private Vertx vertx;
    private MqttCluster mqttCluster;
    private MqttStateService stateService;
    private ClusterDataStore clusterDataStore;
    private Map<Integer,AsyncRequest> responseFutureMap;
    private int requestId=1;


    public ClusterClient(Vertx vertx,  MqttStateService stateService,ClusterDataStore clusterDataStore) {
        this.vertx = vertx;
        this.stateService=stateService;
        this.clusterDataStore=clusterDataStore;
        this.responseFutureMap=new HashMap<>();
    }

    public int nextRequestId(){
        int requestId = this.requestId++;
        clusterDataStore.setRequestId(requestId);
        return requestId;
    }

    public ClusterClient setRequestId(int requestId) {
        this.requestId = requestId;
        return this;
    }

    public Future<Void> init(MqttCluster mqttCluster){
        this.mqttCluster=mqttCluster;
        return clusterDataStore.requestId()
                .onSuccess(this::setRequestId)
                .map((Void)null);
    }

    public void handle(Response response) {
        fireResponse(response.getRequestId(),response);
    }

    public void proposal(int requestId, ActionLog log){
        Buffer buffer = new ProMessage("/addLog", JsonObject.mapFrom(log)).encode();
        ProposalRequest proposalRequest = new ProposalRequest(requestId, buffer);
        proposalRequest.request()
                .setHandler(ar->{
                    if (!ar.succeeded() || !ar.result().isSuccess()) {
                        stateService.addPendingEvent(v->proposal(requestId, log));
                    } else {
                        logger.debug("proposal success log:{}", log);
                    }
                });
    }



    public void takenOverSession(JsonObject body){
        NetCluster net = mqttCluster.net();
        int requestId = nextRequestId();
        Set<String> nodeIds = net.nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());
        mqttCluster.net().request(nodeIds,requestId,new ProMessage("/takenover",body).encode());
    }


    public Future<MessageObj> requestMessage(int requestId,Set<String> nodeIds, String id){
        Promise<MessageObj> promise=Promise.promise();

        ProMessage proMessage = new ProMessage("/message", new JsonObject().put("id", id));
        MultipleRequest multipleRequest = new MultipleRequest( requestId,nodeIds,proMessage.encode());
        multipleRequest.request()
                .setHandler(ar->{
                    try {
                        if (ar.succeeded()&&ar.result().isSuccess()){
                            Response response = ar.result();
                            Buffer payload = response.getPayload();
                            MessageObj messageObj =
                                    new ObjCodec().decodeMessageObj(payload);

                            promise.complete(messageObj);
                        }else{
                            promise.tryFail(ar.cause());
                        }
                    } catch (Exception e) {
                        promise.tryFail(ar.cause());
                    }

                });
        return promise.future();

    }


    public Future<SessionObj> requestSession(int requestId,Set<String> set, String clientId){
        ProMessage proMessage = new ProMessage("/session", new JsonObject().put("clientId", clientId));
        Promise<SessionObj> promise=Promise.promise();
        MultipleRequest multipleRequest = new MultipleRequest( requestId,set,proMessage.encode());
        multipleRequest.request()
                .setHandler(ar->{
                    SessionObj sessionObj = null;
                    if (ar.succeeded()&&ar.result().isSuccess()){
                        Response response = ar.result();
                        // save session
                        Buffer payload = response.getPayload();
                        sessionObj = new ObjCodec().decodeSessionObj(payload);

                    }
                    promise.tryComplete(sessionObj);
                });
        return promise.future();
    }



    public void fireResponse(int requestId, Response response){
        AsyncRequest asyncRequest = responseFutureMap.get(requestId);
        if (asyncRequest!=null)
            asyncRequest.fire(response);
    }

    abstract class AsyncRequest{
        protected int requestId;
        private TimeoutStream timeoutStream;
        protected Promise<Response> promise;

        public AsyncRequest(int requestId) {
            this.requestId = requestId;
            this.promise = Promise.promise();
            this.timeoutStream=vertx.timerStream(5000);
            this.timeoutStream.handler(v->{
                logger.debug("requestId:{} timeout",requestId);
                promise.tryFail("timeout");
            });
            this.promise.future().onComplete(ar->{
                responseFutureMap.remove(requestId);
                timeoutStream.cancel();
            });
            responseFutureMap.put(requestId,this);
        }

        abstract void fire(Response response);


        abstract Future<Response> request();

    }


    class ProposalRequest extends AsyncRequest{
        private Buffer buffer;

        public ProposalRequest(int requestId, Buffer buffer) {
            super(requestId);
            this.buffer = buffer;
        }

        @Override
        public void fire(Response response) {
            promise.tryComplete(response);
        }

        @Override
        Future<Response> request() {
            String leaderId = mqttCluster.getLeaderId();
            String id = mqttCluster.id();

            if (leaderId != null) {
                logger.debug("leaderId:{} id:{}",leaderId,id);
                if (leaderId.equals(id)) {
                    stateService.handle(new RpcMessage(MessageType.REQUEST,id,id,requestId,buffer))
                            .onSuccess(r->fireResponse(requestId,r.setNodeId(id)))
                            .onFailure(t->{
                                logger.error("try add log failed",t);
                                fireResponse(requestId,new Response().setNodeId(id).setSuccess(false));
                            })
                    ;
                } else {
                    mqttCluster.net().request(leaderId,requestId,buffer);
                }
            }else{
                promise.tryFail("election");
            }
            return promise.future();
        }
    }

    class MultipleRequest extends AsyncRequest{
        private Set<String> nodeIds;
        private Buffer buffer;

        public MultipleRequest(int requestId, Set<String> nodeIds, Buffer buffer) {
            super(requestId);
            this.nodeIds = nodeIds;
            this.buffer = buffer;
        }

        @Override
        public void fire( Response response){
            String nodeId = response.getNodeId();
            nodeIds.remove(nodeId);
            if (response.isSuccess()){
                responseFutureMap.remove(requestId);
                promise.tryComplete(response);
            }else{
                if (nodeIds.isEmpty()){
                    promise.tryFail("");
                }
            }
        }

        @Override
        public Future<Response> request() {
            mqttCluster.net().request(nodeIds,requestId,buffer);
            return promise.future();
        }


    }


}
