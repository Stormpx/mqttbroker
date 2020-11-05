package com.stormpx.cluster.mqtt;

import com.stormpx.cluster.LogEntry;
import com.stormpx.cluster.MemberType;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.StateService;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.message.ClusterMessage;
import com.stormpx.cluster.net.ClientExtendRequest;
import com.stormpx.cluster.net.Response;
import com.stormpx.cluster.snapshot.SnapshotContext;
import com.stormpx.cluster.snapshot.SnapshotReader;
import com.stormpx.kit.TopicFilter;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.function.Function;

public class MqttStateService implements StateService {
    private final static Logger logger= LoggerFactory.getLogger(MqttStateService.class);

    private Vertx vertx;
    private MqttCluster mqttCluster;

    private Handler<String> reSetSessionHandler;

    private Map<String, Function<JsonObject,Future<?>>> handlerMap;


    private Map<String,Map<Integer, Promise<Void>>> promiseMap;

    private MqttMetaData mqttMetaData;

    private List<Handler<Void>> pending;


    public MqttStateService(Vertx vertx ) {
        this.vertx = vertx;
        this.handlerMap=new HashMap<>();
        this.mqttMetaData=new MqttMetaData();
        this.pending=new ArrayList<>();
        this.promiseMap =new HashMap<>();
    }

    @Override
    public Future<Void> init(MqttCluster mqttCluster) {
        this.mqttCluster=mqttCluster;

        addHandler("/addLog",body->{
            String nodeId = body.getString("rpc-nodeId");
            Integer requestId = body.getInteger("rpc-requestId",0);
            body.remove("rpc-nodeId");
            body.remove("rpc-requestId");
            Object lowestProposalId = body.remove("lowestProposalId");
            Object proposalId = body.remove("proposalId");

            if (mqttCluster.getMemberType()!= MemberType.LEADER) {
                return Future.<Buffer>succeededFuture();
            }else {
                return addLog(nodeId, (Integer) proposalId,(Integer)lowestProposalId, body.toBuffer())
                        .map(v->Buffer.buffer());
            }
        });


        return Future.succeededFuture();
    }



    @Override
    public void handle(ClientExtendRequest clientExtendRequest) {
        ClusterMessage clusterMessage = clientExtendRequest.getClusterMessage();

        String nodeId = clusterMessage.getFromId();

        RpcMessage prcMessage = RpcMessage.decode(clusterMessage.getBuffer());
        int requestId = prcMessage.getRequestId();
        String res = prcMessage.getRes();
        Function<JsonObject, Future<?>> function = handlerMap.get(res);
        if (function==null){
            clientExtendRequest.response(false,requestId,Buffer.buffer());
        }else{
            JsonObject body = prcMessage.getBody();
            body.put("rpc-nodeId",nodeId)
                .put("rpc-requestId",requestId);

            function.apply(body)
                    .map(o->{
                        if (o==null){
                            return new Response().setSuccess(false);
                        } else if (o instanceof Buffer){
                            return new Response().setSuccess(true).setPayload((Buffer) o);
                        }else if (o instanceof JsonObject){
                            return new Response().setSuccess(true).setPayload( ((JsonObject) o).toBuffer());
                        }else if (o instanceof JsonArray){
                            return new Response().setSuccess(true).setPayload( ((JsonArray) o).toBuffer());
                        }else if (o instanceof String){
                            return new Response().setSuccess(true).setPayload(Buffer.buffer((String) o));
                        }else if (o instanceof Boolean){
                            return new Response().setSuccess((Boolean) o);
                        } else{
                            return new Response().setSuccess(true).setPayload(Json.encodeToBuffer(o));
                        }
                    })
                    .setHandler(ar->{
                        if (ar.succeeded()){
                            Response response = ar.result();
                            clientExtendRequest.response(response.isSuccess(),requestId, response.getPayload());
                        }else{
                            clientExtendRequest.response(false,requestId,Buffer.buffer());
                        }
                    });
        }
    }

    public void addHandler(String resource,Function<JsonObject,Future<?>> handler){
        handlerMap.put(resource,handler);
    }


    public void addPendingEvent(Handler<Void> handler){
        pending.add(handler);
    }


    public Future<Map<String,String>> retainMapWithReadIndex(){
        Promise<Map<String,String>> promise=Promise.promise();
        mqttCluster.readIndex()
                .setHandler(ar->{
                   if (ar.failed()){
                       logger.error("retainMapWithReadIndex failed",ar.cause());
                       pending.add(v-> retainMapWithReadIndex().onComplete(promise));
                   }else{

                       promise.complete(mqttMetaData.getRetainMap());
                   }
                });

        return promise.future();
    }

    public Future<Collection<TopicFilter.MatchResult>> topicMatchesWithReadIndex(String topic){
        Promise<Collection<TopicFilter.MatchResult>> promise=Promise.promise();
        mqttCluster.readIndex()
                .setHandler(ar->{
                    if (ar.failed()){
                        logger.error("topicMatchesWithReadIndex failed",ar.cause());
                        pending.add(v-> topicMatchesWithReadIndex(topic).onComplete(promise));
                    }else {
                        Collection<TopicFilter.MatchResult> matches = mqttMetaData.getTopicFilter().matches(topic);
                        promise.complete(matches);

                    }
                });
        return promise.future();
    }

    public Future<Set<String>> fetchSessionIndexWithReadIndex(String clientId){
        Promise<Set<String>> promise=Promise.promise();
        logger.debug("start fetch session :{} index",clientId);
        mqttCluster.readIndex()
                .setHandler(ar->{
                    if (ar.failed()){
                        logger.error("fetchSessionIndexWithReadIndex failed",ar.cause());
                        pending.add(v-> fetchSessionIndexWithReadIndex(clientId).onComplete(promise));
                    }else{

                        Set<String> set = mqttMetaData.getSessionIndex(clientId);
                        set=set==null?Collections.emptySet():Set.copyOf(set);
                        promise.complete(set);
                    }
                });

        return promise.future();
    }

    public Future<Map<String,Set<String>>> fetchMessageIndexWithReadIndex(Set<String> ids){
        Promise<Map<String,Set<String>>> promise=Promise.promise();
        mqttCluster.readIndex()
                .setHandler(ar->{
                    if (ar.failed()){
                        logger.error("fetchMessageIndexWithReadIndex failed",ar.cause());
                        pending.add(v-> fetchMessageIndexWithReadIndex(ids).onComplete(promise));
                    }else{
                        promise.complete(fetchMessageIndex(ids));
                    }
                });

        return promise.future();
    }
    public Map<String,Set<String>> fetchMessageIndex(Set<String> ids){
        HashMap<String,Set<String>> map = ids.stream().reduce(new HashMap<>(), (m, s) -> {

            Set<String> set = mqttMetaData.getMessageIndex(s);
            if (set != null) m.put(s, Set.copyOf(set));
            return m;
        }, (q1, q2) -> q1);
        return map;
    }
    public Set<String> fetchMessageIndex(String id){
        return Optional.ofNullable(mqttMetaData.getMessageIndex(id)).orElseGet(Collections::emptySet);
    }



    public Future<Void> addLog(String nodeId, int proposalId,int lowestProposalId, Buffer buffer){
        Map<Integer, Promise<Void>> integerPromiseMap = promiseMap.computeIfAbsent(nodeId, k -> new HashMap<>());
        Promise<Void> promise = Promise.promise();
        integerPromiseMap.put(proposalId,promise);
        mqttCluster.addLog(nodeId,proposalId,lowestProposalId,buffer);
        return promise.future();
    }


    public MqttStateService reSetSessionHandler(Handler<String> reSetSessionHandler) {
        this.reSetSessionHandler = reSetSessionHandler;
        return this;
    }

    @Override
    public void firePendingEvent(String leaderId) {
        List<Handler<Void>> pending = this.pending;
        this.pending=new ArrayList<>();
        pending.forEach(handler -> handler.handle(null));
    }

    @Override
    public void applyLog(LogEntry logEntry) {
        Buffer payload = logEntry.getPayload();
        if (payload==null||payload.length()==0)
            return;
        int proposalId = logEntry.getProposalId();
        int lowestProposalId = logEntry.getLowestProposalId();
        if (logger.isDebugEnabled())
            logger.debug("apply index:{} term:{} node:{} proposalId:{} lowestProposalId:{} log:{}",
                    logEntry.getIndex(),logEntry.getTerm(),logEntry.getNodeId(), proposalId,lowestProposalId,payload);
        ActionLog actionLog = Json.decodeValue(payload, ActionLog.class);
        ActionLog.Action action = ActionLog.Action.of(actionLog.getAction());
        if (action==null)
            return;

        String proposalNodeId = logEntry.getNodeId();

        if (lowestProposalId!=0){
            mqttMetaData.discardProposalId(proposalNodeId,lowestProposalId);
        }

        if (proposalId != 0 && mqttMetaData.isExecuted(proposalNodeId, proposalId)) {
            response(proposalNodeId, proposalId);
            return;
        }


        switch (action){
            case SUBSCRIBE:
                List<String> args = actionLog.getArgs();
                String nodeId = args.get(0);
                List<String> list = args.subList(0, args.size());
                mqttMetaData.addSubscription(nodeId,list);
                break;
            case UNSUBSCRIBE:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                list=args.subList(0,args.size());
                mqttMetaData.delSubscription(nodeId,list);
                break;
            case SAVEMESSAGE:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                String messageId = args.get(1);
                String retain = args.get(2);
                if (retain.equals("y")){
                    String topic = args.get(3);
                    int payloadLength = Integer.parseInt(args.get(4));
                    if (payloadLength==0){
                        //del
                        mqttMetaData.removeRetain(topic);
                    }else{
                        mqttMetaData.putRetain(topic,messageId);
                    }
                }
                mqttMetaData.saveMessage(nodeId,messageId);
                break;
            case DELMESSAGE:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                String id = args.get(1);
                mqttMetaData.delMessage(nodeId,id);
                break;
            case SAVESESSION:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                String clientId=args.get(1);
                String reset=args.get(2);

                if ("y".equals(reset)) {
                    mqttMetaData.clearSession(clientId);
                    if (!nodeId.equals(mqttCluster.id())){
                        //FIXME
                        Handler<String> reSetSessionHandler = this.reSetSessionHandler;
                        if (reSetSessionHandler!=null)
                            reSetSessionHandler.handle(clientId);
                    }
                }
                mqttMetaData.saveSession(nodeId,clientId);
                break;
            case DELSESSION:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                clientId=args.get(1);
                mqttMetaData.removeSession(nodeId,clientId);
                break;
            default:
                logger.error("unknown action type?????");
        }

        if (proposalId !=0){
            mqttMetaData.setExecute(proposalNodeId, proposalId);
            response(proposalNodeId, proposalId);
        }
    }

    @Override
    public Future<Void> applySnapshot(SnapshotReader snapshotReader) {
        Promise<Void> promise=Promise.promise();
        snapshotReader.readAll()
                .onFailure(promise::tryFail)
                .onSuccess(buffer->{
                    MqttMetaData mqttMetaData = new MqttMetaData();
                    mqttMetaData.decode(buffer);
                    if (logger.isDebugEnabled()){
                        logger.debug("new snapshot :{}",mqttMetaData);
                    }
                    this.mqttMetaData=mqttMetaData;
                    promise.complete();
                });

        return promise.future();
    }

    @Override
    public void writeSnapshot(SnapshotContext snapshotContext) {
        MqttMetaData metaData = mqttMetaData.copy();
        snapshotContext.getWriter()
                .onFailure(t->logger.error("try get snapshot writer failed",t))
                .onSuccess(writer->{
                    vertx.<Void>executeBlocking(p->{
                        writer.write(metaData.encodeSubscribe());
                        writer.write(metaData.encodeRequestId());
                        writer.write(metaData.encodeRetain());
                        writer.write(metaData.encodeIdIndex());
                        writer.write(metaData.encodeSessionIndex());
                        writer.end().setHandler(p);
                    },ar->{
                        if (ar.failed()){
                            logger.error("bug write snapshot failed ",ar.cause());
                        }
                    });

                });
    }

    private void response(String nodeId,int proposalId){
        Map<Integer, Promise<Void>> map = promiseMap.get(nodeId);
        if (map!=null){
            Promise<Void> promise = map.remove(proposalId);
            if (promise!=null)
                promise.tryComplete();
        }
    }


}
