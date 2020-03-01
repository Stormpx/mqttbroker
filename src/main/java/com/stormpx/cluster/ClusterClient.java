package com.stormpx.cluster;

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
    private Map<Integer,MultipleRequest> responseFutureMap;
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
        fire(response.getRequestId(),response);
    }

    public void proposal(int requestId, ActionLog log){
        String leaderId = mqttCluster.getLeaderId();
        String id = mqttCluster.id();

        if (leaderId != null) {
            Buffer buffer = new ProMessage("/addLog", JsonObject.mapFrom(log)).encode();
            MultipleRequest request = new MultipleRequest(Set.of(leaderId), requestId);
            request.future()
                    .setHandler(ar -> {
                        if (!ar.succeeded() || !ar.result().isSuccess()) {
                            stateService.addPendingEvent(v->proposal(requestId, log));
                        } else {
                            logger.debug("proposal log:{}", log);
                        }
                    });

            logger.debug("leaderId:{} id:{}",leaderId,id);
            if (leaderId.equals(id)) {
                stateService.handle(new RpcMessage(MessageType.REQUEST,id,id,requestId,buffer))
                        .onSuccess(r->fire(requestId,r.setNodeId(id)))
                        .onFailure(t->{
                            logger.error("try add log failed",t);
                            fire(requestId,new Response().setNodeId(id).setSuccess(false));
                        })
                        ;
            } else {
                mqttCluster.net().request(leaderId,requestId,buffer);
            }
        }else{
            stateService.addPendingEvent(v-> proposal(requestId,log));
        }
    }



    public void takenOverSession(JsonObject body){
        NetCluster net = mqttCluster.net();
        int requestId = nextRequestId();
        Set<String> nodeIds = net.nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());
        mqttCluster.net().request(nodeIds,requestId,new ProMessage("/takenover",body).encode());
    }


    public Future<MessageObj> requestMessage(int requestId,Set<String> nodeIds, String id){
        ProMessage proMessage = new ProMessage("/message", new JsonObject().put("id", id));

        mqttCluster.net().request(nodeIds,requestId,proMessage.encode());
        Promise<MessageObj> promise=Promise.promise();
        MultipleRequest multipleRequest = new MultipleRequest(nodeIds, requestId);
        multipleRequest.future()
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

        mqttCluster.net().request(set,requestId,proMessage.encode());
        Promise<SessionObj> promise=Promise.promise();
        MultipleRequest multipleRequest = new MultipleRequest(set, requestId);
        multipleRequest.future()
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



    public void fire(int requestId,Response response){
        MultipleRequest multipleRequest = responseFutureMap.get(requestId);
        if (multipleRequest!=null)
            multipleRequest.resp(response);
    }



    class MultipleRequest{
        private Set<String> nodeIds;
        private int requestId;
        private TimeoutStream timeoutStream;
        private Promise<Response> promise;

        public MultipleRequest(Set<String> nodeIds, int requestId) {
            this.nodeIds = new HashSet<>(nodeIds);
            this.requestId = requestId;
            this.promise=Promise.promise();
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



        public void resp(Response response){
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

        public Future<Response> future(){
            return promise.future();
        }

    }


}
