package com.stormpx;

import com.stormpx.cluster.ClusterClient;
import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.MqttStateService;
import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.message.ProMessage;
import com.stormpx.kit.J;
import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.TopicUtil;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.*;
import com.stormpx.store.rocksdb.RocksDBClusterDataStore;
import com.stormpx.store.rocksdb.RocksDBMessageStore;
import com.stormpx.store.rocksdb.RocksDBSessionStore;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.function.Predicate;
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

        this.topicFilter=new TopicFilter();

        String saveDir = config.getString(Constants.SAVE_DIR);
        if (saveDir==null||saveDir.isBlank()){
            startFuture.tryFail("save dir is empty");
            return;
        }
        logger.info("save dir :{}",saveDir);
        this.messageStore=new RocksDBMessageStore(vertx,saveDir);
        this.sessionStore=new RocksDBSessionStore(vertx,saveDir);



        JsonObject cluster = config.getJsonObject("cluster");
        if (cluster==null)
            cluster=new JsonObject();

        String id = cluster.getString("id");
        Integer port = cluster.getInteger("port");
        JsonObject nodes = cluster.getJsonObject("nodes");
        if (id==null||port==null||port<0||port>66535||nodes==null||nodes.isEmpty()){
            this.cluster=false;
            logger.info("cluster disable");
        }else {
            //cluster enable
            this.clusterDataStore=new RocksDBClusterDataStore(vertx,saveDir,id);
            this.stateService=new MqttStateService(vertx,messageStore,sessionStore);

//            this.stateService.addHandler("/store/sync",this::);
//            this.stateService.addHandler("/session/sync",);
            this.stateService.addHandler("/session",this::requestSession);
            this.stateService.addHandler("/message",this::requestMessage);
            this.stateService.addHandler("/takenover",this::takenOverSession);
            this.stateService.addHandler("/dispatcher",this::dispatcherMsg);

            this.clusterClient=new ClusterClient(vertx,stateService,messageStore,clusterDataStore);
            this.mqttCluster=new MqttCluster(vertx,cluster,clusterDataStore,stateService,clusterClient);
            this.cluster=true;
            logger.info("cluster enable id:{} port:{} nodes:{}",id,port,nodes);
        }


        vertx.eventBus().<String>localConsumer("_client_session_present")
                .handler(message->{
                    String clientId = message.body();
                    sessionPresent(clientId)
                            .setHandler(ar->{
                               if (ar.succeeded()){
                                   message.reply(ar.result());
                               }else{
                                   logger.error("",ar.cause());
                                   message.fail(500,ar.cause().getMessage());
                               }
                            });
                });


        vertx.eventBus().<JsonObject>localConsumer("_client_accept_")
                .handler(message->{
                    JsonObject body = message.body();
                    String clientId = body.getString("clientId");
                    Boolean cleanSession = body.getBoolean("cleanSession",false);
                    if (this.cluster)
                        clusterClient.proposal(clusterClient.nextRequestId(),ActionLog.saveSession(mqttCluster.id(),clientId, cleanSession));
                });


        vertx.eventBus().<JsonObject>consumer("_session_taken_over_")
                .handler(message->{
                    JsonObject body = message.body();
                    if (this.cluster)
                        clusterClient.takenOverSession(body);
                });


        vertx.eventBus().<UnSafeJsonObject>localConsumer("_message_dispatcher_")
                .handler(message->{
                    JsonObject body = message.body().getJsonObject();
                    messageDispatcher(body);
                });


        vertx.eventBus().<String>localConsumer("_topic_reload_")
                .handler(message->{
                    String clientId = message.body();
                    fetchSubscription(clientId)
                        .onFailure(t->message.fail(500,t.getMessage()))
                        .onSuccess(jsonArray->{
                            message.reply(jsonArray);
                            topicSubscribe(new JsonObject().put("clientId",clientId).put("subscriptions",jsonArray),true);
                        });
                });

        vertx.eventBus().<JsonObject>localConsumer("_topic_subscribe_")
                .handler(message->{
                    JsonObject body = message.body();
                    topicSubscribe(body,false).setHandler(ar->{
                        if (ar.succeeded()){
                            message.reply(null);
                        }else{
                            logger.error("topicSubscribe failed",ar.cause());
                            message.fail(500,ar.cause().getMessage());
                        }
                    });
                });

        vertx.eventBus().<JsonObject>localConsumer("_topic_unSubscribe_")
                .handler(message->{
                    JsonObject body = message.body();
                    topicUnSubscribe(body)
                        .setHandler(ar->{
                            if (ar.succeeded()){
                                message.reply(null);
                            }else{
                                logger.error("topicUnSubscribe failed",ar.cause());
                                message.fail(500,ar.cause().getMessage());
                            }
                        });

                });

        vertx.eventBus().<JsonObject>localConsumer("_retain_match_")
                .handler(message->{
                    JsonObject jsonObject = message.body();
                    JsonArray jsonArray = jsonObject.getJsonArray("topicFilters");
                    String address = jsonObject.getString("address");
                    List<String> topicFilters = jsonArray.stream().map(Object::toString).collect(Collectors.toList());

                    retainMatch(address,topicFilters);

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
                    storeOperation(action, (JsonObject) body)
                        .setHandler(ar->{
                            if (ar.succeeded()){
                                message.reply(ar.result());
                            }else{
                                logger.error("storeOperation",ar.cause());
                                message.fail(500,ar.cause().getMessage());
                            }
                        });
                });



        if (this.mqttCluster!=null)
            this.mqttCluster.start().setHandler(startFuture);
        else
            startFuture.complete();


    }

    private Future<?> storeOperation(String action,JsonObject body){
        switch (action){
            case "clearSession":
                return clearSession(body);
            case "setExpiryTimestamp":
                return setExpiryTimestamp(body);
            case "getExpiryTimestamp":
                return getExpiryTimestamp(body);
            case "link":
                return link(body);
            case "release":
                return release(body);
            case "receive":
                return receive(body);
            case "addPacketId":
                return addPacketId(body);
            case "unacknowledgedPacketId":
                return unacknowledgedPacketId(body).map(JsonArray::new);
            case "removePacketId":
                return removePacketId(body);
            case "storeWillMessage":
                return storeWillMessage(body);
            case "fetchWillMessage":
                return fetchWillMessage(body);
            case "dropWillMessage":
                return dropWillMessage(body);
        }
        return Future.failedFuture("no action");
    }


    private Future<JsonArray> fetchSubscription(String clientId) {
        return sessionStore.fetchSubscription(clientId);
    }

    private Future<List<Integer>> unacknowledgedPacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.packetId(clientId);

    }


    private Future<Void> clearSession(JsonObject body) {
        Promise<Void> promise=Promise.promise();
        String clientId = body.getString("clientId");
        Future<Void> releaseLink = releaseLink(clientId);
        Future<Void> future=sessionStore.fetchSubscription(clientId)
                    .compose(array->{
                        if (array==null)
                            return Future.succeededFuture();
                        List<String> topicFilter = J.toJsonStream(array).map(json -> json.getString("topicFilter")).collect(Collectors.toList());
                        return topicUnSubscribe(new JsonObject().put("clientId",clientId).put("topics",topicFilter));
                    });

        CompositeFuture.all(releaseLink,future)
                .setHandler(ar->{
                    if (ar.succeeded()){
                        sessionStore.del(clientId).onComplete(promise);
                        if (cluster){
                            clusterClient.proposal(clusterClient.nextRequestId(),ActionLog.delSession(mqttCluster.id(),clientId));
                        }
                    }else{
                        promise.fail(ar.cause());
                    }
                });
        return promise.future();
    }

    private Future<Void> releaseLink(String clientId){
        Promise<Void> promise=Promise.promise();
        sessionStore.links(clientId)
                .onFailure(t->{
                    logger.error("clear seesion : {} fail ",clientId);
                    //                   clearSession(body);
                    promise.fail(t);
                })
                .onSuccess(list->{
                    if (list==null) {
                        promise.complete();
                        return;
                    }
                    List<Future> futures=list.stream().filter(json->json.containsKey("id"))
                            .map(json->json.getString("id"))
                            .map(id-> modifyRefCnt(id,-1))
                            .collect(Collectors.toList());
                    CompositeFuture.all(futures)
                            .onComplete(ar->{
                                if (ar.succeeded()){
                                    promise.complete();
                                }else{
                                    promise.fail(ar.cause());
                                }
                            });

                });

        return promise.future();
    }




    private Future<Void> setExpiryTimestamp(JsonObject body) {
        String clientId = body.getString("clientId");
        Long expiryTimestamp = body.getLong("expiryTimestamp");

        return sessionStore.setExpiryTimestamp(clientId,expiryTimestamp);

    }

    private Future<Long> getExpiryTimestamp(JsonObject body) {
        String clientId = body.getString("clientId");

        return sessionStore.getExpiryTimestamp(clientId);
    }

    private Future<Void> link(JsonObject link) {
        String clientId = link.getString("clientId");
        String id = link.getString("id");
        return sessionStore.addLink(clientId,link)
                .onSuccess(v->modifyRefCnt(id,+1))
                .map((Void)null)
                .onFailure(t->logger.error("link :{} fail",t,link));

    }

    /**
     * remove message link
     * @param body
     */
    private Future<Void> release(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        return sessionStore.release(clientId, packetId)
                .onSuccess(id->{
                    if (id!=null){
                        modifyRefCnt(id, -1);
                    }
                })
                .map((Void)null)
                .onFailure(t->logger.error("release client:{} link :{} fail",t,clientId,packetId));

    }

    /**
     * remove message link.id
     * @param body
     */
    private Future<Void> receive(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        return sessionStore.receive(clientId, packetId)
                .onSuccess(id->{
                    if (id!=null){
                        modifyRefCnt(id, -1);
                    }
                })
                .map((Void)null)
                .onFailure(t->logger.error("set client :{} link receive :{} fail",t,clientId,packetId));

    }



    private Future<Void> addPacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        return sessionStore.addPacketId(clientId,packetId);

    }

    private Future<Void> removePacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        return sessionStore.removePacketId(clientId,packetId);
    }

    private Future<JsonObject> fetchWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.getWill(clientId);
    }

    private Future<Void> storeWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        JsonObject will = body.getJsonObject("will");
        return sessionStore.saveWill(clientId,will);
    }

    private Future<Void> dropWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.delWill(clientId);
    }

    private void messageDispatcher(JsonObject message){
        String topic = message.getString("topic");
        Boolean retain = message.getBoolean("retain");
        if (!cluster){
           dispatcherEvent(message.put("unlimited",true));
        }else{

            stateService.topicMatchesWithReadIndex(topic)
                    .onSuccess(c->{
                        logger.debug("match list :{}",c);
                        int requestId = clusterClient.nextRequestId();
                        Set<String> sendSet=new HashSet<>();
                        for (TopicFilter.SubscribeInfo subscribeInfo : c) {
                            JsonArray shareTopics = subscribeInfo.getAllMatchSubscribe()
                                    .stream()
                                    .filter(TopicFilter.Entry::isShare)
                                    .map(TopicFilter.Entry::getTopicFilterName)
                                    .collect(J.toJsonArray());

                            JsonObject json = message.copy().put("shareTopics", shareTopics);

                            String nodeId = subscribeInfo.getClientId();
                            sendSet.add(nodeId);
                            if (nodeId.equals(mqttCluster.id())){
                                dispatcherEvent(json);
                            }else {
                                mqttCluster.net().request(nodeId, requestId, new ProMessage("/dispatcher", json).encode());
                            }
                        }
                        if (retain){
                            mqttCluster.net().nodes().stream().map(ClusterNode::id).filter(Predicate.not(sendSet::contains)).forEach(id->{
                                mqttCluster.net().request(id, requestId, new ProMessage("/dispatcher", message).encode());
                            });
                        }
                    });
        }
    }



    private void dispatcherEvent(JsonObject message) {

        String id = message.getString("id");
        Integer qos = message.getInteger("qos");
        String topic = message.getString("topic");
        Boolean retain = message.getBoolean("retain",false);
        byte[] payloads = message.getBinary("payload");
        boolean match = topicFilter.anyMatch(topic);
        if (match||retain) {
            if (qos > 0) {
                if (match || payloads.length != 0) {
                    messageStore.set(id, new MessageObj(message));
                    if (cluster) {
                        clusterClient.proposal(clusterClient.nextRequestId(), ActionLog.saveMessage(mqttCluster.id(), id, retain, topic, payloads.length));
                    }
                }
            }
            if (retain) {
                messageStore.putRetain(topic, payloads.length == 0||qos==0 ? null : id).setHandler(ar -> {
                    if (ar.succeeded()) {
                        String result = ar.result();
                        if (result != null && !result.equals(id)) {
                            modifyRefCnt(result, -1);
                        }
                        modifyRefCnt(id, +1);
                    } else {
                        logger.error("put retain topic:{} id:{} fail", ar.cause(), topic, id);
                    }
                });
            }

            }
        if (match) {
            JsonObject copy = message.copy();
            vertx.eventBus().publish("_mqtt_message_dispatcher", UnSafeJsonObject.wrapper(copy));
        }

    }

    private Future<Integer> modifyRefCnt(String id, int delta){
        return messageStore.addAndGetRefCnt(id,delta)
                .onFailure(t->{})
                .onSuccess(i->{
                   if (i!=null&&i<=0){
                       messageStore.del(id);
                       if (cluster){
                           clusterClient.proposal(clusterClient.nextRequestId(),ActionLog.delMessage(mqttCluster.id(),id));
                       }
                   }
                });
    }


    private Future<Boolean> sessionPresent(String clientId){
        Promise<Boolean> promise=Promise.promise();
        if (!cluster){
            localSession(clientId)
                        .setHandler(promise);
        }else{
            stateService.fetchSessionIndexWithReadIndex(clientId)
                    .onSuccess(set->{
                        if (set.isEmpty()) {
                            promise.complete(false);
                            return;
                        }
                        if (set.contains(mqttCluster.id())){
                            localSession(clientId).setHandler(promise);
                        }else {
                            clusterClient.requestSession(clusterClient.nextRequestId(), new HashSet<>(set), clientId)
                                    .onFailure(promise::fail)
                                    .onSuccess(sessionObj->{
                                        if (sessionObj == null || sessionObj.getExpiryTimestamp() == null) {
                                            promise.complete(false);
                                        } else {
                                            Long expiryTimestamp = sessionObj.getExpiryTimestamp();
                                            boolean isExpiry = Instant.now().getEpochSecond() < expiryTimestamp;
                                            if (!isExpiry) {
                                                sessionStore.save(sessionObj);
                                            }
                                            promise.complete(isExpiry);
                                        }
                                    });
                        }
                    });
            }
        return promise.future();
    }

    private Future<Boolean> localSession(String clientId){
        return sessionStore.getExpiryTimestamp(clientId)
                .compose(timeStamp->{
                    if (timeStamp==null||Instant.now().getEpochSecond()>=timeStamp){
                        return clearSession(new JsonObject().put("clientId",clientId))
                            .map(false);
                    }else{
                        return Future.succeededFuture(true);
                    }
                });
    }

    private Future<Void> topicSubscribe(JsonObject body,boolean reload){
        Promise<Void> promise=Promise.promise();
        String clientId = body.getString("clientId");
        JsonArray subscriptions = body.getJsonArray("subscriptions",J.EMPTY_ARRAY);
        J.toJsonStream(subscriptions).map(json->json.getString("topicFilter")).forEach(topicFilter->{
            this.topicFilter.subscribe(topicFilter,clientId, MqttQoS.AT_MOST_ONCE,false,false,0);
        });
        Future<Void> future=Future.succeededFuture();
        if (!reload)
            future=sessionStore.addSubscription(clientId,subscriptions);


        return future
                .onSuccess(v->{
                    if (cluster){
                        ActionLog log = ActionLog.subscribe(mqttCluster.id(), J.toJsonStream(subscriptions).map(json->json.getString("topicFilter")).collect(Collectors.toList()));
                        clusterClient.proposal(clusterClient.nextRequestId(),log);
                    }
                })
                .onComplete(promise);


    }

    private Future<Void> topicUnSubscribe(JsonObject body){
        Promise<Void> promise=Promise.promise();
        String clientId = body.getString("clientId");
        JsonArray topics = body.getJsonArray("topics",J.EMPTY_ARRAY);
        List<String> list = topics.stream()
                .map(Object::toString)
                .peek(topicFilter->this.topicFilter.unSubscribe(topicFilter,clientId))
                .collect(Collectors.toList());

        sessionStore.deleteSubscription(clientId,list)
                .onSuccess(v->{
                    if (cluster){
                        List<String> unSubscribeList = list.stream().filter(topicFilter -> !this.topicFilter.anySubscribed(topicFilter)).collect(Collectors.toList());
                        if (!unSubscribeList.isEmpty()) {
                            ActionLog log = ActionLog.unSubscribe(mqttCluster.id(), unSubscribeList);
                            clusterClient.proposal(clusterClient.nextRequestId(), log);
                        }
                    }
                })
                .onComplete(promise);

        return promise.future();
    }

    private void retainMatch(String address,List<String> topicFilters){

        if (!cluster){
            messageStore.retainMap()
                    .onFailure(t->logger.error("fetch retainMapWithReadIndex fail",t))
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
                    .retainMapWithReadIndex()
                    .map(map->map.keySet()
                            .stream()
                            .filter(topic->topicFilters.stream().anyMatch(o-> TopicUtil.matches(o,topic)))
                            .map(map::get)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toSet()))

                    .map(stateService::fetchMessageIndex)
                    .onFailure(t->logger.error("fetch message fail",t))
                    .onSuccess(map -> {

                        if (map.isEmpty()) {
                            return;
                        }

                        JsonArray array = new JsonArray();
                        List<Map.Entry<String, Set<String>>> missIdList = map.entrySet()
                                .stream()
                                .peek(e -> array.add(e.getKey()))
                                .filter(e -> !e.getValue().contains(mqttCluster.id()))
                                .collect(Collectors.toList());

                        List<Future> futures = missIdList.stream().map(e -> {
                            int requestId = clusterClient.nextRequestId();
                            return clusterClient.requestMessage(requestId, e.getValue(), e.getKey())
                                    .onSuccess(msg->{
                                        if (!isExpiry(msg)){
                                            messageStore.set(e.getKey(),msg);
                                            clusterClient.proposal(clusterClient.nextRequestId(),ActionLog.saveMessage(mqttCluster.id(),e.getKey(),false,null,0));
                                        }
                                    });
                        }).collect(Collectors.toList());

                        CompositeFuture.all(futures).onComplete(ar -> {
                            // logger
                            if (ar.failed()){
                                logger.error("request message fail",ar.cause());

                            }
                            logger.debug("match id :{}", array.encode());

                            array.forEach(o->{
                                messageStore.get(o.toString())
                                        .onSuccess(msg->{
                                            if (msg!=null)
                                                vertx.eventBus().send(address,UnSafeJsonObject.wrapper(msg.getMessage().copy().put("retain",true)));
                                        });
                            });

                        });
            });
        }
    }

    private void resendUnReleaseMessage(String address,String clientId) {
        sessionStore.links(clientId)
                .onFailure(t->logger.error("resendUnReleaseMessage fail",t))
                .onSuccess(list->{
                    for (JsonObject link : list) {
                        String id = link.getString("id");
                        Integer packetId = link.getInteger("packetId");
                        if (id==null){
                            if (packetId !=null){
                                vertx.eventBus().send(address,UnSafeJsonObject.wrapper(link));
                            }
                        }else{
                            messageStore.get(id)
                                    .onFailure(t->logger.error("get message: {} fail cause:{}",id,t.getMessage()))
                                    .onSuccess(msgObj->{
                                        if (msgObj!=null) {
                                            JsonObject msg = msgObj.getMessage();
                                            if (packetId==null&&isExpiry(msgObj)) {
                                                return;
                                            }
                                            JsonObject message = msg.copy().mergeIn(link);
                                            vertx.eventBus().send(address,UnSafeJsonObject.wrapper(message.put("dup",true)));
                                        } else {
                                            if (!cluster) {
                                                if (packetId!=null)
                                                    sessionStore.release(clientId, packetId);
                                            } else{
                                                stateService.fetchMessageIndexWithReadIndex(Set.of(id))
                                                        .onSuccess(map->{
                                                            Set<String> set = map.get(id);
                                                            set.remove(clientId);
                                                            if (set.isEmpty()){
                                                                if (packetId!=null)
                                                                    sessionStore.release(clientId, packetId);
                                                                return;
                                                            }
                                                            clusterRequestAndSend(address,set,id,link);
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
                    if (!isExpiry(messageObj)) {
                        messageStore.set(id,messageObj);
                        clusterClient.proposal(clusterClient.nextRequestId(),ActionLog.saveMessage(mqttCluster.id(),id,false,null,0));
                        vertx.eventBus().send(address, message.copy().mergeIn(link).put("dup", true));
                    }
                });
    }

    private boolean isExpiry(MessageObj messageObj){
        JsonObject message = messageObj.getMessage();
        Long expiryTimestamp = message.getLong("expiryTimestamp");
        if (expiryTimestamp==null)
            return false;
        return Instant.now().getEpochSecond()>=expiryTimestamp;
    }


    private Future<Buffer> requestSession(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.get(clientId)
                .map(sessionObj -> sessionObj==null?null:new ObjCodec().encodeSessionObj(sessionObj))
                .onFailure(t->logger.error("get client:{} session fail ",t,clientId));
    }

    private Future<Buffer> requestMessage(JsonObject body){
        String id = body.getString("id");
        return messageStore.get(id)
                .map(messageObj -> messageObj==null?null:new ObjCodec().encodeMessageObj(messageObj))
                .onFailure(t->logger.error("get message id:{} fail",t,id));
    }

    private Future<Boolean> takenOverSession(JsonObject body){
        String clientId = body.getString("clientId");
        Boolean sessionEnd = body.getBoolean("sessionEnd");
        if (sessionEnd){
            sessionStore.del(clientId);
        }
        vertx.eventBus().publish("_mqtt_session_taken_over", body);
        return Future.succeededFuture(true);
    }

    private Future<Boolean> dispatcherMsg(JsonObject body){
        body.remove("rpc-nodeId");
        body.remove("rpc-requestId");
        dispatcherEvent(body);
        return Future.succeededFuture(true);
    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        stopFuture.complete();
    }
}
