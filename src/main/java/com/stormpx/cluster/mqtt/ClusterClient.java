package com.stormpx.cluster.mqtt;

import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.message.*;
import com.stormpx.cluster.net.NetCluster;
import com.stormpx.cluster.net.Response;
import com.stormpx.store.*;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
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

    private ProposalIdentifier proposalIdentifier;

    private int requestId;

    public ClusterClient(Vertx vertx,  MqttStateService stateService,ClusterDataStore clusterDataStore) {
        this.vertx = vertx;
        this.stateService=stateService;
        this.clusterDataStore=clusterDataStore;
        this.responseFutureMap=new HashMap<>();
    }

    class ProposalIdentifier{
        private int lowestProposalId;
        private int proposalId;
        private Set<Integer> set;

        public ProposalIdentifier(int proposalId) {
            this.proposalId = proposalId;
            this.lowestProposalId=proposalId-1;
            this.set=new HashSet<>();
        }

        public int nextId(){
            int proposalId = this.proposalId++;
            clusterDataStore.setRequestId(proposalId);
            return proposalId;
        }

        public int getLowestId(){
            return lowestProposalId;
        }

        public void resp(int proposalId){
            if (proposalId-lowestProposalId==1){
                lowestProposalId=proposalId;
                while (set.contains(lowestProposalId+1)&&lowestProposalId+1<this.proposalId){
                    lowestProposalId+=1;
                    set.remove(lowestProposalId);
                }
            }else{
                set.add(proposalId);
            }
        }

    }

    public int nextRequestId(){
        return requestId++;
    }

    public Future<Void> init(MqttCluster mqttCluster){
        this.mqttCluster=mqttCluster;
        return clusterDataStore.requestId()
                .onSuccess(id-> this.proposalIdentifier=new ProposalIdentifier(id==null?1:id))
                .map((Void)null);
    }

    public void handle(Response response) {
        fireResponse(response.getRequestId(),response);
    }


    public void proposal( ActionLog log){
        proposal(this.proposalIdentifier.nextId(),log);
    }


    public void proposal(int proposalId, ActionLog log){
        ProposalRequest proposalRequest = new ProposalRequest(nextRequestId(),proposalId,this.proposalIdentifier.getLowestId(), log);
        proposalRequest.request()
                .setHandler(ar->{
                    if (!ar.succeeded() || !ar.result().isSuccess()) {
                        stateService.addPendingEvent(v->proposal(proposalId, log));
                    } else {
                        logger.debug("proposal success log:{}", log);
                        this.proposalIdentifier.resp(proposalId);
                    }
                });
    }



    public void takenOverSession(JsonObject body){
        NetCluster net = mqttCluster.net();
        int requestId = nextRequestId();
        Set<String> nodeIds = net.nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());
        mqttCluster.net().request(nodeIds,new RpcMessage("/takenover",requestId,body).encode());
    }


    public Future<MessageObj> requestMessage(int requestId,Set<String> nodeIds, String id){
        Promise<MessageObj> promise=Promise.promise();
        MultipleRequest multipleRequest = new MultipleRequest("/message", requestId,nodeIds,new JsonObject().put("id", id));
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
        Promise<SessionObj> promise=Promise.promise();
        MultipleRequest multipleRequest = new MultipleRequest("/session", requestId,set,new JsonObject().put("clientId", clientId));
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
            //FIXME debug
            this.timeoutStream=vertx.timerStream(3000);
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


    }


    class ProposalRequest extends AsyncRequest{
        private int lowestId;
        private int proposalId;
        private ActionLog actionLog;

        public ProposalRequest(int requestId,int proposalId, int lowestId,ActionLog actionLog) {
            super(requestId);
            this.proposalId=proposalId;
            this.lowestId=lowestId;
            this.actionLog = actionLog;
        }

        @Override
        public void fire(Response response) {
            promise.tryComplete(response);
        }

        private Future<Response> request() {
            String leaderId = mqttCluster.getLeaderId();
            String id = mqttCluster.id();

            if (leaderId != null) {
                logger.debug("leaderId:{} id:{}",leaderId,id);
                if (leaderId.equals(id)) {
                    stateService.addLog(id,proposalId,lowestId, Json.encodeToBuffer(actionLog))
                            .onSuccess(r->fireResponse(requestId,new Response().setSuccess(true).setNodeId(id)))
                            .onFailure(t->{
                                logger.error("try add log failed",t);
                                fireResponse(requestId,new Response().setNodeId(id).setSuccess(false));
                            })
                    ;
                } else {
                    Buffer buffer = new RpcMessage("/addLog", requestId,
                            JsonObject.mapFrom(actionLog).put("lowestProposalId",lowestId).put("proposalId", this.proposalId)).encode();
                    mqttCluster.net().request(leaderId,buffer);
                }
            }else{
                promise.tryFail("election");
            }
            return promise.future();
        }
    }

    class MultipleRequest extends AsyncRequest{
        private String res;
        private Set<String> nodeIds;
        private JsonObject body;

        public MultipleRequest(String res,int requestId, Set<String> nodeIds, JsonObject body) {
            super(requestId);
            this.res=res;
            this.nodeIds = nodeIds;
            this.body=body;
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

        private Future<Response> request() {
            RpcMessage rpcMessage = new RpcMessage(res, requestId, body);
            mqttCluster.net().request(nodeIds,rpcMessage.encode());
            return promise.future();
        }


    }


}
