package com.stormpx.cluster;

import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.net.Request;
import com.stormpx.cluster.net.Response;
import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.DataStorage;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;

import java.util.*;

public class MqttStateHandler  implements StateHandler{
    private Vertx vertx;
    private JsonObject config;
    private DataStorage dataStorage;
    private TopicFilter topicFilter;

    //key topic value id
    private Map<String,String> retainMap;
    //key id value nodeIds
    private Map<String, Set<String>> idIndexMap;

    //key clientId value nodeIds
    private Map<String,Set<String>> sessionMap;

    public MqttStateHandler(Vertx vertx, JsonObject config,DataStorage dataStorage) {
        this.vertx = vertx;
        this.config = config;
        this.dataStorage=dataStorage;
        this.topicFilter=new TopicFilter();
        this.retainMap=new HashMap<>();
        this.idIndexMap=new HashMap<>();
        this.sessionMap=new HashMap<>();
        init();
    }

    private void init(){
        vertx.eventBus().registerDefaultCodec(LogEntry.class,LogEntry.CODEC);
        vertx.eventBus().<UnSafeJsonObject>localConsumer("dis",msg->{

        });
    }

    @Override
    public Future<ClusterState> loadClusterState() {
        return null;
    }

    @Override
    public void handle(Request request) {

    }

    @Override
    public void handle(Response response) {

    }

    @Override
    public void onLeaderHeartbeat(String leaderId) {

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
                if ("y".equals(reset))
                    clientIdIndexSet.clear();

                clientIdIndexSet.add(nodeId);
                break;
        }
    }
}
