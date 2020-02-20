package com.stormpx.cluster;

import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.message.RequestType;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.net.Request;
import com.stormpx.cluster.net.Response;
import com.stormpx.kit.J;
import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.kit.value.Values2;
import com.stormpx.store.DataStorage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class MqttStateHandler  implements StateHandler{
    private final static Logger logger= LoggerFactory.getLogger(MqttStateHandler.class);

    private Vertx vertx;
    private JsonObject config;
    private String id;
    private MqttCluster mqttCluster;
    private DataStorage dataStorage;
    private TopicFilter topicFilter;


    private int requestId=1;



    private Map<String,Map<Integer, Promise<Void>>> mapMap;

    //key nodeId value requestId
    private Map<String,Set<Integer>> idempotentMap;

    //key topic value id
    private Map<String,String> retainMap;
    //key id value nodeIds
    private Map<String, Set<String>> idIndexMap;
    //key clientId value nodeIds
    private Map<String,Set<String>> sessionMap;


    private Client client;

    private List<Handler<Void>> pending;

    public MqttStateHandler(Vertx vertx, JsonObject config,DataStorage dataStorage) {
        this.vertx = vertx;
        this.config = config;
        this.id=config.getString("nodeId");
        this.dataStorage=dataStorage;
        this.topicFilter=new TopicFilter();
        this.retainMap=new HashMap<>();
        this.idIndexMap=new HashMap<>();
        this.sessionMap=new HashMap<>();
        this.idempotentMap=new HashMap<>();
        this.client=new Client();
        this.pending=new ArrayList<>();
        init();
    }

    private int nextRequestId(){
        return requestId++;
    }

    private void messageDispatcher(int requestId,JsonObject message){
        String topic = message.getString("topic");
        mqttCluster.readonlySafeFuture()
                .setHandler(ar->{
                    if (ar.failed()){
                        pending.add(v->messageDispatcher(requestId,message));
                    }else {
                        Collection<TopicFilter.SubscribeInfo> matches = this.topicFilter.matches(topic);
                        for (TopicFilter.SubscribeInfo subscribeInfo : matches) {
                            JsonArray shareTopics = subscribeInfo.getAllMatchSubscribe().stream().filter(TopicFilter.Entry::isShare).map(TopicFilter.Entry::getTopicFilterName).collect(J.toJsonArray());
                            String nodeId = subscribeInfo.getClientId();
                            mqttCluster.net().request(nodeId, requestId, message.copy().put("shareTopics", shareTopics).toBuffer());
                        }
                    }
                });
    }

    private void fetchSessionIndex(String clientId,Handler<Set<String>> sessionIndexHandler){
        mqttCluster.readonlySafeFuture()
                .setHandler(ar->{
                    if (ar.failed()){
                        pending.add(v->fetchSessionIndex(clientId,sessionIndexHandler));
                    }else{
                        Set<String> set = sessionMap.get(clientId);
                        set=set==null?Collections.emptySet():Set.copyOf(set);
                        sessionIndexHandler.handle(set);
                    }
                });
    }

    private void requestSession(int requestId,Set<String> set, String clientId,Promise<Void> promise){
        mqttCluster.net().request(set,requestId,Buffer.buffer().appendByte((byte) RequestType.SESSION.getValue()).appendString(clientId,"utf-8"));
        Future<Response> future = client.future(300,requestId);
        future.setHandler(ar->{
           if (ar.failed()||!ar.result().isSuccess()){
               requestSession(requestId,set,clientId,promise);
           }else{
               //todo save session
               promise.tryComplete();
           }
        });
    }

    private void fetchMessageIndex(String clientId,Handler<Set<String>> messageIndexHandler){
        mqttCluster.readonlySafeFuture()
                .setHandler(ar->{
                    if (ar.failed()){
                        pending.add(v->fetchMessageIndex(clientId,messageIndexHandler));
                    }else{
                        Set<String> set = idIndexMap.get(clientId);
                        set=set==null?Collections.emptySet():Set.copyOf(set);
                        messageIndexHandler.handle(set);
                    }
                });
    }

    private void init(){
        vertx.eventBus().registerDefaultCodec(LogEntry.class,LogEntry.CODEC);
        vertx.eventBus().<UnSafeJsonObject>localConsumer("_mqtt_message_dispatcher")
                .handler(message->{
                    JsonObject body = message.body().getJsonObject();
                    messageDispatcher(nextRequestId(),body);
                });
        vertx.eventBus().<JsonArray>localConsumer("_cluster_subscribe")
                .handler(message->{
                    JsonArray jsonArray = message.body();
                    ActionLog log = ActionLog.subscribe(id, jsonArray.stream().map(Object::toString).collect(Collectors.toList()));
                    requestAddLog(nextRequestId(),log);

                });

        vertx.eventBus().<JsonArray>localConsumer("_cluster_unSubscribe")
                .handler(message->{
                    JsonArray jsonArray = message.body();
                    ActionLog log = ActionLog.unSubscribe(id, jsonArray.stream().map(Object::toString).collect(Collectors.toList()));
                    requestAddLog(nextRequestId(),log);
                });

        vertx.eventBus().<String>localConsumer("_cluster_request_message")
                .handler(message->{
                    String clientId = message.body();

                    fetchSessionIndex(clientId,set->{
                        if (set.isEmpty()||set.contains(id)) {
                            message.reply(null);
                            return;
                        }
                        Promise<Void> promise=Promise.promise();
                        requestSession(nextRequestId(),new HashSet<>(set),clientId,promise);
                    });

                });


        vertx.eventBus().<String>localConsumer("_cluster_request_session")
                .handler(message->{
                    String clientId = message.body();

                    fetchSessionIndex(clientId,set->{
                        if (set.isEmpty()||set.contains(id)) {
                            message.reply(null);
                            return;
                        }
                        Promise<Void> promise=Promise.promise();
                        requestSession(nextRequestId(),new HashSet<>(set),clientId,promise);
                    });

                });
    }


    public void requestAddLog(int requestId,ActionLog log){
        mqttCluster.propose(requestId,Json.encodeToBuffer(log));
        Future<Response> future = client.future(300,requestId);
        future.setHandler(ar->{
            if (!ar.succeeded() || !ar.result().isSuccess()) {
                requestAddLog(requestId,log);
            }else{
                logger.debug("propose log:{}",log);
            }
        });

    }



    @Override
    public Future<ClusterState> loadClusterState() {
        return dataStorage.getState()
                .compose(state-> dataStorage.logs().map(list-> Values2.values(state,list)))
                .map(v->{
                    JsonObject state = v.getOne();
                    List<LogEntry> logEntryList = v.getTwo();
                    ClusterState clusterState = new ClusterState();
                    if (state!=null){
                        this.requestId=state.getInteger("requestId");
                        clusterState.setCurrentTerm(state.getInteger("term"));
                        clusterState.setLastIndex(state.getInteger("lastIndex"));
                        clusterState.setCommitIndex(state.getInteger("commitIndex"));
                        clusterState.setLastApplied(state.getInteger("lastApplied"));
                    }
                    if (logEntryList!=null){
                        int commitIndex = clusterState.getCommitIndex();
                        logEntryList.stream()
                                .sorted(Comparator.comparingInt(LogEntry::getIndex))
                                .forEachOrdered(log->{
                                    if (log.getIndex()<=commitIndex) {
                                        clusterState.setLog(log);
                                        executeLog(log);
                                    }
                                });
                    }
                    return clusterState;
                });
    }

    @Override
    public void handle(Request request) {
        RpcMessage rpcMessage = request.getRpcMessage();

        int requestId = rpcMessage.getRequestId();
        String nodeId = rpcMessage.getFromId();

        Buffer buffer = rpcMessage.getBuffer();
        RequestType requestType = RequestType.valueOf(buffer.getByte(0));
        if (requestType==null)
            return;
        switch (requestType){
            case SESSION:

                break;
            case MESSAGE:

                break;
            case TAKENOVER:
                vertx.eventBus().publish("_mqtt_session_taken_over",buffer.slice(1,buffer.length()));
                request.response(true,Buffer.buffer());
                break;
            case PUBLISH:
                JsonObject message = buffer.slice(1, buffer.length()).toJsonObject();
                vertx.eventBus().publish("_mqtt_message_dispatcher", UnSafeJsonObject.wrapper(message));
                request.response(true,Buffer.buffer());
                break;
            case ADDLOG:
                Map<Integer, Promise<Void>> integerPromiseMap = mapMap.computeIfAbsent(nodeId, k -> new HashMap<>());
                Promise<Void> promise = Promise.promise();
                integerPromiseMap.put(requestId,promise);
                promise.future()
                        .setHandler(ar->{
                            if (ar.succeeded()){
                                request.response(true,Buffer.buffer());
                            }else{
                                request.response(false,Buffer.buffer());
                            }
                        });
                mqttCluster.addLog(nodeId,requestId,buffer.slice(1,buffer.length()));
                break;
        }
        //sub unsub saveid savesession --> leader wait headrtbeat  resp after commit
        //dis retainMessage sessionIndex messageIndex req -->exec&resp after headrtbeat
        //messagereq sessionreq takenoversession --> respimmd

    }

    @Override
    public void handle(Response response) {
        client.fire(response.getRequestId(),response);
    }

    @Override
    public void firePending(String leaderId) {
        List<Handler<Void>> pending = this.pending;
        this.pending=new ArrayList<>();
        pending.forEach(handler -> handler.handle(null));
    }

    @Override
    public void saveState(ClusterState clusterState) {
        JsonObject json = new JsonObject();
        json.put("term",clusterState.getCurrentTerm());
        json.put("lastIndex",clusterState.getLastIndex());
        json.put("commitIndex",clusterState.getCommitIndex());
        json.put("lastApplied",clusterState.getLastApplied());
        dataStorage.saveState(json);
    }

    @Override
    public void saveLog(LogEntry logEntry) {
        JsonObject json = new JsonObject();
        json.put("index",logEntry.getIndex());
        json.put("term",logEntry.getTerm());
        json.put("payload",logEntry.getPayload().getBytes());
        dataStorage.saveLog(logEntry);
    }

    @Override
    public void delLog(int start, int end) {
        dataStorage.delLog(start,end);
    }

    @Override
    public void executeLog(LogEntry logEntry) {
        Buffer payload = logEntry.getPayload();
        if (payload==null)
            return;
        ActionLog actionLog = Json.decodeValue(payload, ActionLog.class);
        ActionLog.Action action = ActionLog.Action.of(actionLog.getAction());
        if (action==null)
            return;
        if (logEntry.getRequestId()!=0){
            String nodeId = logEntry.getNodeId();
            Set<Integer> set = idempotentMap.computeIfAbsent(nodeId, k -> new HashSet<>());
            if (set.contains(logEntry.getRequestId())) {
                response(nodeId,logEntry.getRequestId());
                return;
            }
        }
        switch (action){
            case SUBSCRIBE:
                List<String> args = actionLog.getArgs();
                String nodeId = args.get(0);
                ListIterator<String> listIterator = args.listIterator(1);
                listIterator.forEachRemaining(topicFilter->{
                    this.topicFilter.subscribe(nodeId,topicFilter, MqttQoS.EXACTLY_ONCE,false,false,0);
                });
                break;
            case UNSUBSCRIBE:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                listIterator = args.listIterator(1);
                listIterator.forEachRemaining(topicFilter->{
                    this.topicFilter.unSubscribe(nodeId,topicFilter);
                });
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
                        retainMap.remove(topic);
                    }else{
                        retainMap.put(topic,messageId);
                    }
                }
                Set<String> idIndexSet = idIndexMap.computeIfAbsent(messageId, k -> new HashSet<>());
                idIndexSet.add(nodeId);
                break;
            case DELMESSAGE:
                //todo
                break;
            case SAVESESSION:
                args = actionLog.getArgs();
                nodeId = args.get(0);
                String clientId=args.get(1);
                String reset=args.get(2);
                Set<String> clientIdIndexSet = sessionMap.computeIfAbsent(clientId, k -> new HashSet<>());
                if ("y".equals(reset)) {
                    clientIdIndexSet.clear();

                }
                clientIdIndexSet.add(nodeId);
                break;
        }
        if (logEntry.getRequestId()!=0){
            String nodeId = logEntry.getNodeId();
            idempotentMap.get(nodeId).add(logEntry.getRequestId());
            response(nodeId,logEntry.getRequestId());
        }
    }

    private void response(String nodeId,int requestId){
        Map<Integer, Promise<Void>> map = mapMap.get(nodeId);
        if (map!=null){
            Promise<Void> promise = map.remove(requestId);
            if (promise!=null)
                promise.tryComplete();
        }
    }


    private class Client{
        private Map<Integer,Promise<Response>> responseFutureMap;

        public Client() {
            this.responseFutureMap=new HashMap<>();
        }

        public Future<Response> future(long timeout,int requestId){
            Promise<Response> promise=Promise.promise();
            responseFutureMap.put(requestId,promise);
            TimeoutStream timeoutStream = vertx.timerStream(timeout);
            timeoutStream.handler(id->{
                promise.tryFail("timeout");
            });
            return promise.future().onComplete(c->timeoutStream.cancel());
        }


        public void fire(int requestId,Response response){
            Promise<Response> promise = responseFutureMap.remove(requestId);
            if (promise!=null)
                promise.tryComplete(response);
        }

    }
}
