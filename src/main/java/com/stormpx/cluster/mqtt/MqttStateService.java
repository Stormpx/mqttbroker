package com.stormpx.cluster.mqtt;

import com.stormpx.cluster.LogEntry;
import com.stormpx.cluster.MemberType;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.StateService;
import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.message.ProMessage;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.net.Response;
import com.stormpx.cluster.snapshot.SnapshotContext;
import com.stormpx.cluster.snapshot.SnapshotReader;
import com.stormpx.kit.TopicFilter;
import com.stormpx.store.SessionStore;
import io.netty.handler.codec.mqtt.MqttQoS;
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
            if (mqttCluster.getMemberType()!= MemberType.LEADER) {
                return Future.<Buffer>succeededFuture();
            }else {
                return addLog(nodeId, requestId, body.toBuffer())
                        .map(v->Buffer.buffer());
            }
        });


        return Future.succeededFuture();
    }



    @Override
    public Future<Response> handle(RpcMessage rpcMessage) {
        int requestId = rpcMessage.getRequestId();
        String nodeId = rpcMessage.getFromId();

        ProMessage proMessage = ProMessage.decode(rpcMessage.getBuffer());

//        Promise<Response> promise=Promise.promise();

        String res = proMessage.getRes();
        Function<JsonObject, Future<?>> function = handlerMap.get(res);
        if (function==null){
            return Future.succeededFuture(new Response().setSuccess(false));
        }else{
            JsonObject body = proMessage.getBody();
            body.put("rpc-nodeId",nodeId)
                .put("rpc-requestId",requestId);

            return function.apply(body)
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
                    });
        }
        /*JsonObject body = proMessage.getBody();
        switch (proMessage.getRequestType()){
            case SESSION:
                String clientId = body.getString("clientId");
                sessionStore.get(clientId)
                        .setHandler(ar->{
                            if (ar.succeeded()&&ar.result()!=null){
                                Buffer buffer = new ObjCodec().encodeSessionObj(ar.result());
                                promise.complete(new Response().setSuccess(true).setPayload(buffer));
                            }else{
                                logger.error("get session fail ",ar.cause());
                                promise.complete(new Response().setSuccess(false));
                            }
                        });
                break;
            case MESSAGE:
                String id = body.getString("id");
                messageStore.get(id)
                        .setHandler(ar->{
                            if (ar.succeeded()&&ar.result()!=null){
                                promise.complete(new Response().setSuccess(true).setPayload(new ObjCodec().encodeMessageObj(ar.result())));
                            }else{
                                logger.error("get message id:{} fail",ar.cause());
                                promise.complete(new Response().setSuccess(false));
                            }
                        });
                break;
            case TAKENOVER:
                Handler<JsonObject> takeoverHandler = this.takeoverHandler;
                if (takeoverHandler!=null)
                    takeoverHandler.handle(body);
                promise.complete(new Response().setSuccess(true));
                break;
            case PUBLISH:
                Handler<JsonObject> publishHandler = this.publishHandler;
                if (publishHandler!=null){
                    publishHandler.handle(body);
                }
                promise.complete(new Response().setSuccess(true));
                break;
            case ADDLOG:
                if (mqttCluster.getMemberType()!=MemberType.LEADER) {
                    promise.complete(new Response().setSuccess(false));
                }else {
                    addLog(nodeId, requestId, body.toBuffer())
                            .onFailure(promise::tryFail)
                            .onSuccess(v -> promise.tryComplete(new Response().setSuccess(true)));
                }
                break;
        }
        //sub unsub saveid savesession --> leader wait headrtbeat  resp after commit
        //dis retainMessage sessionIndex messageIndex req -->exec&resp after headrtbeat
        //messagereq sessionreq takenoversession --> respimmd

        return promise.future();*/
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

    public Future<Collection<TopicFilter.SubscribeInfo>> topicMatchesWithReadIndex(String topic){
        Promise<Collection<TopicFilter.SubscribeInfo>> promise=Promise.promise();
        mqttCluster.readIndex()
                .setHandler(ar->{
                    if (ar.failed()){
                        logger.error("topicMatchesWithReadIndex failed",ar.cause());
                        pending.add(v-> topicMatchesWithReadIndex(topic).onComplete(promise));
                    }else {
                        Collection<TopicFilter.SubscribeInfo> matches = mqttMetaData.getTopicFilter().matches(topic);
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



    private Future<Void> addLog(String nodeId, int requestId, Buffer buffer){
        Map<Integer, Promise<Void>> integerPromiseMap = promiseMap.computeIfAbsent(nodeId, k -> new HashMap<>());
        Promise<Void> promise = Promise.promise();
        integerPromiseMap.put(requestId,promise);
        mqttCluster.addLog(nodeId,requestId,buffer);
        return promise.future();
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
        int requestId = logEntry.getRequestId();
        if (logger.isDebugEnabled())
            logger.debug("apply index:{} term:{} node:{} requestId:{} log:{}",
                    logEntry.getIndex(),logEntry.getTerm(),logEntry.getNodeId(), requestId,payload);
        ActionLog actionLog = Json.decodeValue(payload, ActionLog.class);
        ActionLog.Action action = ActionLog.Action.of(actionLog.getAction());
        if (action==null)
            return;

        if (requestId !=0){
            String nodeId = logEntry.getNodeId();
            if (mqttMetaData.isExecuted(nodeId, requestId)) {
                response(nodeId, requestId);
                return;
            }
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
                        //todo
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

        if (requestId !=0){
            String nodeId = logEntry.getNodeId();
            mqttMetaData.setExecute(nodeId, requestId);
            response(nodeId, requestId);
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

    private void response(String nodeId,int requestId){
        Map<Integer, Promise<Void>> map = promiseMap.get(nodeId);
        if (map!=null){
            Promise<Void> promise = map.remove(requestId);
            if (promise!=null)
                promise.tryComplete();
        }
    }


}
