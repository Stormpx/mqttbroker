package com.stormpx.cluster;

import com.stormpx.cluster.message.*;
import com.stormpx.cluster.net.Response;
import com.stormpx.store.MessageStore;
import com.stormpx.store.SessionStore;
import com.stormpx.store.MessageObj;
import com.stormpx.store.ObjCodec;
import com.stormpx.store.SessionObj;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class ClusterClient {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);
    private Vertx vertx;
    private MqttCluster mqttCluster;
    private MqttStateService stateService;
    private MessageStore messageStore;
    private SessionStore sessionStore;
    private Map<Integer,MultipleRequest> responseFutureMap;
    private int requestId=1;

    private List<Handler<Void>> pending;

    public ClusterClient(Vertx vertx,  MqttStateService stateService) {
        this.vertx = vertx;
        this.stateService=stateService;
        this.responseFutureMap=new HashMap<>();
    }

    public int nextRequestId(){
        return requestId++;
    }

    public ClusterClient setRequestId(int requestId) {
        this.requestId = requestId;
        return this;
    }

    public void init(MqttCluster mqttCluster){
        this.mqttCluster=mqttCluster;

    }

    public void handle(Response response) {
        fire(response.getRequestId(),response);
    }

    public void propose(int requestId,ActionLog log){
        String leaderId = mqttCluster.getLeaderId();
        String id = mqttCluster.id();

        if (leaderId != null) {
            Buffer buffer = new ProMessage(RequestType.ADDLOG, JsonObject.mapFrom(log)).encode();
            MultipleRequest request = new MultipleRequest(Set.of(leaderId), requestId);
            request.future()
                    .setHandler(ar -> {
                        if (!ar.succeeded() || !ar.result().isSuccess()) {
                            propose(requestId, log);
                        } else {
                            logger.debug("propose log:{}", log);
                        }
                    });

            if (leaderId.equals(id)) {
                stateService.handle(
                        new RpcMessage()
                                .setMessageType(MessageType.REQUEST)
                                .setRequestId(requestId)
                                .setFromId(id)
                                .setBuffer(buffer))
                        .onSuccess(r->fire(requestId,r.setNodeId(id)))
                        .onFailure(t->{
                            logger.error("try add log fail",t);
                            fire(requestId,new Response().setNodeId(id).setSuccess(false));
                        })
                        ;
            } else {
                mqttCluster.net().request(leaderId,requestId,buffer);
            }
        }else{
            stateService.addPendingEvent(v->propose(requestId,log));
        }
    }



    public Future<MessageObj> requestMessage(int requestId,Set<String> nodeIds, String id){
        ProMessage proMessage = new ProMessage(RequestType.MESSAGE, new JsonObject().put("id", id));

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
                            messageStore.set(id,messageObj);

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


    public Future<Void> requestSession(int requestId,Set<String> set, String clientId){
        ProMessage proMessage = new ProMessage(RequestType.SESSION, new JsonObject().put("clientId", clientId));

        mqttCluster.net().request(set,requestId,proMessage.encode());
        Promise<Void> promise=Promise.promise();
        MultipleRequest multipleRequest = new MultipleRequest(set, requestId);
        multipleRequest.future()
                .setHandler(ar->{
                    if (ar.succeeded()&&ar.result().isSuccess()){
                        Response response = ar.result();
                        //todo save session
                        Buffer payload = response.getPayload();
                        SessionObj sessionObj =
                                new ObjCodec().decodeSessionObj(payload);

                        sessionStore.save(sessionObj);

                    }
                    promise.tryComplete();
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
            this.timeoutStream=vertx.timerStream(300);
            this.timeoutStream.handler(v->promise.tryFail("timeout"));
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
