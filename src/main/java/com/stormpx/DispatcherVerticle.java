package com.stormpx;

import com.stormpx.cluster.ClusterClient;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.MqttStateService;
import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.message.ProMessage;
import com.stormpx.cluster.message.RequestType;
import com.stormpx.kit.J;
import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.TopicUtil;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.ClusterDataStore;
import com.stormpx.store.MessageStore;
import com.stormpx.store.SessionStore;
import com.stormpx.store.MessageObj;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class DispatcherVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);

    private boolean cluster;
    private MessageStore messageStore;
    private SessionStore sessionStore;
    private ClusterDataStore clusterDataStore;

    private MqttCluster mqttCluster;
    private MqttStateService stateService;
    private ClusterClient clusterClient;

    private TopicFilter topicFilter;

    @Override
    public void start(Future<Void> startFuture) throws Exception {

        JsonObject config = Optional.ofNullable(config()).orElse(new JsonObject());
        JsonObject cluster = config.getJsonObject("cluster");
        String id = cluster.getString("id");
        Integer port = cluster.getInteger("port");
        JsonObject nodes = cluster.getJsonObject("nodes");
        if (id==null||port==null||port<0||port>66535||nodes==null||nodes.isEmpty()){
            this.cluster=false;

        }else {
            //cluster enable
            this.stateService=new MqttStateService(vertx,messageStore,sessionStore);
            this.stateService.publishHandler(this::dispatcherEvent)
                            .takeoverHandler(this::takenOverSession);
            this.clusterClient=new ClusterClient(vertx,stateService);
            this.mqttCluster=new MqttCluster(vertx,cluster,clusterDataStore,stateService,clusterClient);
            this.cluster=true;
        }

        vertx.eventBus().<JsonObject>localConsumer("_client_accept_")
                .handler(message->{
                    JsonObject body = message.body();
                    String clientId = body.getString("clientId");
                    Boolean cleanSession = body.getBoolean("cleanSession",false);
                    clusterClient.propose(clusterClient.nextRequestId(),ActionLog.saveSession(mqttCluster.id(),clientId, cleanSession));
                });

        vertx.eventBus().<UnSafeJsonObject>localConsumer("_message_dispatcher_")
                .handler(message->{
                    JsonObject body = message.body().getJsonObject();
                    messageDispatcher(body);
                });

        vertx.eventBus().<JsonObject>localConsumer("_topic_subscribe_")
                .handler(message->{
                    JsonObject body = message.body();
                    topicSubscribe(body);
                    message.reply(null);
                });

        vertx.eventBus().<JsonObject>localConsumer("_topic_unSubscribe_")
                .handler(message->{
                    JsonObject body = message.body();
                    topicUnSubscribe(body);
                    message.reply(null);
                });

        vertx.eventBus().<JsonObject>localConsumer("_retain_match_")
                .handler(message->{
                    JsonObject jsonObject = message.body();
                    JsonArray jsonArray = jsonObject.getJsonArray("topicFilters");
                    String address = jsonObject.getString("address");
                    List<String> topicFilters = jsonArray.stream().map(Object::toString).collect(Collectors.toList());

                    retainMatch(address,topicFilters);

                });

        vertx.eventBus().<String>localConsumer("_request_message")
                .handler(message->{

                });


        vertx.eventBus().<String>localConsumer("_request_session")
                .handler(message->{
                    String clientId = message.body();

                    stateService.fetchSessionIndex(clientId)
                            .onSuccess(set->{
                                if (set.isEmpty()||set.contains(mqttCluster.id())) {
                                    message.reply(null);
                                    return;
                                }
                                clusterClient.requestSession(clusterClient.nextRequestId(),new HashSet<>(set),clientId)
                                        .onComplete(ar->{
                                            message.reply(null);
                                        });
                            });

                });

        vertx.eventBus().<JsonObject>localConsumer("_message_resend_")
                .handler(message->{
                    JsonObject body = message.body();
                    String address = body.getString("address");
                    String clientId = body.getString("clientId");
                    if (address==null||clientId==null)
                        return;
                    resendUnReleaseMessage(address,clientId);

                });

        vertx.eventBus().consumer("_mqtt_store_")
                .handler(message->{
                    String action = message.headers().get("action");
                    if (action==null)
                        return;
                    Object body = message.body();
                    logger.debug("receive request action:{} body:{}",action,body);
                    switch (action){
                        case "clearSession":
                            clearSession((JsonObject) body);
                            break;
                        case "setExpiryTimestamp":
                            setExpiryTimestamp((JsonObject) body);
                            break;
                        case "getExpiryTimestamp":
                            getExpiryTimestamp((JsonObject)body)
                                .setHandler(ar->{
                                    if (ar.succeeded()){
                                        message.reply(ar.result());
                                    }else{
                                        message.fail(500,"getExpiryTimestamp fail");
                                    }
                                })
                                ;
                            break;
                        case "link":
                            link((JsonObject)body);
                            break;
                        case "release":
                            release((JsonObject)body);
                            break;
                        case "receive":
                            receive((JsonObject)body);
                            break;
                        case "fetchSubscription":
                            fetchSubscription((JsonObject)body)
                                    .setHandler(ar->{
                                        if (ar.succeeded()){
                                            message.reply(ar.result());
                                        }else {
                                            message.fail(500,"fetchSubscription fail");
                                        }
                                    });

                            break;
                        case "addPacketId":
                            addPacketId((JsonObject)body);
                            break;
                        case "unacknowledgedPacketId":
                            unacknowledgedPacketId((JsonObject)body)
                                    .setHandler(ar->{
                                       if (ar.succeeded()){
                                           message.reply(new JsonArray(ar.result()));
                                       }else{
                                           message.fail(500,"get unacknowledgedPacketId fail");
                                       }
                                    });

                            break;
                        case "removePacketId":
                            removePacketId((JsonObject)body);
                            break;
                        case "storeWillMessage":
                            storeWillMessage((JsonObject)body);
                            break;
                        case "fetchWillMessage":
                            fetchWillMessage((JsonObject)body)
                                    .setHandler(ar->{
                                        if (ar.succeeded()){
                                            message.reply(ar.result());
                                        }else{
                                            message.fail(500,"fetchWillMessage fail");
                                        }
                                    });
                            break;
                        case "dropWillMessage":
                            dropWillMessage((JsonObject)body);
                            break;
                    }
                });



        if (this.mqttCluster!=null)
            this.mqttCluster.start().setHandler(startFuture);
        else
            startFuture.complete();
    }

    private Future<JsonObject> fetchWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.getWill(clientId);
    }

    private void resendUnReleaseMessage(String address,String clientId) {
        sessionStore.links(clientId)
                .onFailure(t->logger.error("resendUnReleaseMessage fail",t))
                .onSuccess(list->{
                    for (JsonObject jsonObject : list) {
                        String id = jsonObject.getString("id");
                        if (id==null){
                            if (jsonObject.getInteger("packetId")!=null){
                                vertx.eventBus().send(address,UnSafeJsonObject.wrapper(jsonObject));
                            }
                        }else{
                            messageStore.get(id)
                                    .onFailure(t->logger.error("get message: {} fail cause:{}",id,t.getMessage()))
                                    .onSuccess(msgObj->{
                                        if (msgObj!=null) {
                                            JsonObject msg = msgObj.getMessage();
                                            JsonObject message = msg.copy().mergeIn(jsonObject);
                                            vertx.eventBus().send(address,UnSafeJsonObject.wrapper(message.put("dup",true)));
                                        }
                                        else {
                                            if (!cluster) {
                                                sessionStore.release(clientId, jsonObject.getInteger("packetId"));
                                            } else{
                                                stateService.fetchMessageIndex(Set.of(id))
                                                        .onSuccess(map->{
                                                            Set<String> set = map.get(id);
                                                            if (set.isEmpty()){
                                                               sessionStore.release(clientId, jsonObject.getInteger("packetId"));
                                                               return;
                                                           }
                                                           clusterRequestAndSend(address,set,id,jsonObject);
                                                        });
                                            }
                                        }
                                    });
                        }
                    }
                });
    }

    private void clusterRequestAndSend(String address,Set<String> nodeIds, String id,JsonObject link){
        clusterClient.requestMessage(clusterClient.nextRequestId(),nodeIds,id)
                .onFailure(t->logger.error("request message fail",t))
                .onSuccess(messageObj->{
                    JsonObject message = messageObj.getMessage();
                    vertx.eventBus().send(address,message.copy().mergeIn(link).put("dup",true));
                });
    }


    private Future<JsonArray> fetchSubscription(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.fetchSubscription(clientId);
    }

    private Future<List<Integer>> unacknowledgedPacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.packetId(clientId);

    }


    private void clearSession(JsonObject body) {
        String clientId = body.getString("clientId");
        sessionStore.del(clientId);
        if (cluster){
            clusterClient.propose(clusterClient.nextRequestId(),ActionLog.delSession(mqttCluster.id(),clientId));

        }
    }

    private void setExpiryTimestamp(JsonObject body) {

        String clientId = body.getString("clientId");
        Long expiryTimestamp = body.getLong("expiryTimestamp");

        sessionStore.setExpiryTimestamp(clientId,expiryTimestamp);

    }

    private Future<Long> getExpiryTimestamp(JsonObject body) {
        String clientId = body.getString("clientId");

        return sessionStore.getExpiryTimestamp(clientId);
    }

    private void link(JsonObject link) {
        String clientId = link.getString("clientId");
        String id = link.getString("id");
        sessionStore.addLink(clientId,link);
        //todo
    }

    /**
     * remove message link
     * @param body
     */
    private void release(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        sessionStore.release(clientId, packetId)
            .setHandler(ar->{
                if (ar.succeeded()){
                    String id = ar.result();
                }else{

                }
            });
        //ackPacket id != null
        /*if (id!=null)
            publishMessageStore.add(id,-1);*/
    }

    /**
     * remove message link.id
     * @param body
     */
    private void receive(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        sessionStore.receive(clientId, packetId)
                .setHandler(ar->{
                    if (ar.succeeded()){
                        String id = ar.result();
                    }else{

                    }
                });
        /*if (id!=null)
            publishMessageStore.add(id,-1);*/
    }

    private void addPacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        sessionStore.addPacketId(clientId,packetId);

    }

    private void removePacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        sessionStore.removePacketId(clientId,packetId);
    }


    private void storeWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        JsonObject will = body.getJsonObject("will");
        sessionStore.saveWill(clientId,will);
    }

    private void dropWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        sessionStore.delWill(clientId);
    }

    private void messageDispatcher(JsonObject message){
        String topic = message.getString("topic");
        if (!cluster){
           dispatcherEvent(message);
        }else{
            stateService.topicMatches(topic)
                    .onSuccess(c->{
                        int requestId = clusterClient.nextRequestId();
                        for (TopicFilter.SubscribeInfo subscribeInfo : c) {
                            JsonArray shareTopics = subscribeInfo.getAllMatchSubscribe().stream().filter(TopicFilter.Entry::isShare).map(TopicFilter.Entry::getTopicFilterName).collect(J.toJsonArray());
                            JsonObject json = message.copy().put("shareTopics", shareTopics);
                            String nodeId = subscribeInfo.getClientId();
                            if (nodeId.equals(mqttCluster.id())){
                                dispatcherEvent(json);
                            }else {
                                mqttCluster.net().request(nodeId, requestId, new ProMessage(RequestType.PUBLISH, json).encode());
                            }
                        }
                    });
        }
    }

    private void takenOverSession(JsonObject body){
        String clientId = body.getString("clientId");
        Boolean sessionEnd = body.getBoolean("sessionEnd");
        if (sessionEnd){
            sessionStore.del(clientId);
        }
        vertx.eventBus().publish("_mqtt_session_taken_over", body);
    }


    private void dispatcherEvent(JsonObject message) {
        String id = message.getString("id");
        String topic = message.getString("topic");
        Boolean retain = message.getBoolean("retain",false);
        byte[] payloads = message.getBinary("payload");
        boolean match = topicFilter.anyMatch(topic);
        if (match||retain){
            if (match || payloads.length != 0){
                messageStore.set(id, new MessageObj(message));
                if (cluster){
                    clusterClient.propose(clusterClient.nextRequestId(),ActionLog.saveMessage(mqttCluster.id(),id,retain,topic,payloads.length));
                }
            }
            if (retain){
                messageStore.putRetain(topic,payloads.length==0?null:id);
            }

        }

        if (match) {
            vertx.eventBus().publish("_mqtt_message_dispatcher", UnSafeJsonObject.wrapper(message));
        }

    }


    private void topicSubscribe(JsonObject body){
        String clientId = body.getString("clientId");
        JsonArray subscriptions = body.getJsonArray("subscriptions",J.EMPTY_ARRAY);
        J.toJsonStream(subscriptions).map(json->json.getString("topicFilter")).forEach(topicFilter->{
            this.topicFilter.subscribe(topicFilter,clientId, MqttQoS.AT_MOST_ONCE,false,false,0);
        });
        sessionStore.addSubscription(clientId,subscriptions);

        if (cluster){
            ActionLog log = ActionLog.subscribe(mqttCluster.id(), J.toJsonStream(subscriptions).map(json->json.getString("topicFilter")).collect(Collectors.toList()));
            clusterClient.propose(clusterClient.nextRequestId(),log);
        }
    }

    private void topicUnSubscribe(JsonObject body){
        String clientId = body.getString("clientId");
        JsonArray topics = body.getJsonArray("topics",J.EMPTY_ARRAY);
        List<String> list = topics.stream()
                .map(Object::toString)
                .peek(topicFilter->this.topicFilter.unSubscribe(topicFilter,clientId))
                .collect(Collectors.toList());
        sessionStore.deleteSubscription(clientId,list);

        if (cluster){
            ActionLog log = ActionLog.unSubscribe(mqttCluster.id(), list);
            clusterClient.propose(clusterClient.nextRequestId(),log);
        }
    }

    private void retainMatch(String address,List<String> topicFilters){

        if (!cluster){
            messageStore.retainMap()
                    .onFailure(t->logger.error("fetch retainMap fail",t))
                    .onSuccess(map->{
                       map.entrySet()
                               .stream()
                               .filter(e->topicFilters.stream().anyMatch(o-> TopicUtil.matches(o,e.getKey())))
                               .map(Map.Entry::getValue)
                               .filter(Objects::nonNull)
                               .forEach(id->{
                                   messageStore.get(id)
                                           .map(MessageObj::getMessage)
                                           .onSuccess(msg->{
                                               vertx.eventBus().send(address,UnSafeJsonObject.wrapper(msg.copy().put("retain",true)));
                                           });
                               });
                    });
        }else {
            stateService
                    .retainMap(topicFilters)
                    .map(map->map.keySet()
                            .stream()
                            .filter(topic->topicFilters.stream().anyMatch(o-> TopicUtil.matches(o,topic)))
                            .map(map::get)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet()))

                    .compose(stateService::fetchMessageIndex)
                    .onFailure(t->logger.error("fetch message fail",t))
                    .onSuccess(map -> {

                        if (map.isEmpty()) {
                            return;
                        }

                        JsonArray array = new JsonArray();
                        List<Map.Entry<String, Set<String>>> list = map.entrySet()
                                .stream()
                                .peek(e -> array.add(e.getKey()))
                                .filter(e -> !e.getValue().contains(mqttCluster.id()))
                                .collect(Collectors.toList());

                        List<Future> futures = list.stream().map(e -> {
                            int requestId = clusterClient.nextRequestId();
                            return clusterClient.requestMessage(requestId, e.getValue(), e.getKey());
                        }).collect(Collectors.toList());

                        CompositeFuture.all(futures).onComplete(ar -> {
                            // logger
                            if (ar.failed()){
                                logger.error("request message fail",ar.cause());

                            }
                            logger.debug("match id :{}", array.encode());

                            array.forEach(o->{
                                messageStore.get(o.toString())
                                        .map(MessageObj::getMessage)
                                        .onSuccess(msg->{
                                            vertx.eventBus().send(address,UnSafeJsonObject.wrapper(msg.copy().put("retain",true)));
                                        });
                            });

                        });
            });
        }
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {

    }
}
