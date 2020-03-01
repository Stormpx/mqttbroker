package com.stormpx.cluster;

import com.stormpx.Constants;
import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.message.ProMessage;
import com.stormpx.kit.TopicUtil;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.ClusterDataStore;
import com.stormpx.store.ObjCodec;
import com.stormpx.store.rocksdb.RocksDBClusterDataStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

public class ClusterVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);

    private ClusterDataStore clusterDataStore;

    private MqttCluster mqttCluster;
    private MqttStateService stateService;
    private ClusterClient clusterClient;
    @Override
    public void start(Future<Void> startFuture) throws Exception {
        JsonObject config = Optional.ofNullable(config()).orElse(new JsonObject());
        String saveDir = config.getString(Constants.SAVE_DIR);
        if (saveDir==null||saveDir.isBlank()){
            startFuture.tryFail("save dir is empty");
            return;
        }
        JsonObject cluster = config.getJsonObject("cluster");
        if (cluster==null)
            cluster=new JsonObject();

        String id = cluster.getString("id");
        Integer port = cluster.getInteger("port");
        JsonObject nodes = cluster.getJsonObject("nodes");
        //cluster enable
        this.clusterDataStore=new RocksDBClusterDataStore(vertx,saveDir,id);
        this.stateService=new MqttStateService(vertx);

        //            this.stateService.addHandler("/store/sync",this::);
        //            this.stateService.addHandler("/session/sync",);
        this.stateService.addHandler("/session",this::requestSession);
        this.stateService.addHandler("/message",this::requestMessage);
        this.stateService.addHandler("/takenover",this::takenOverSession);
        this.stateService.addHandler("/dispatcher",this::dispatcherMsg);

        this.clusterClient=new ClusterClient(vertx,stateService,clusterDataStore);
        this.mqttCluster=new MqttCluster(vertx,cluster,clusterDataStore,stateService,clusterClient);
        logger.info("cluster enable id:{} port:{} nodes:{}",id,port,nodes);


        vertx.eventBus().<JsonObject>consumer("_session_taken_over_")
                .handler(message->{
                    JsonObject body = message.body();

                    clusterClient.takenOverSession(body);
                });

        vertx.eventBus().<ActionLog>consumer("_log_proposal_")
                .handler(message->{
                    ActionLog actionLog = message.body();
                    clusterClient.proposal(clusterClient.nextRequestId(),actionLog);
                });


        vertx.eventBus().<String>consumer("_session_request_")
                .handler(message->{
                    String clientId = message.body();
                    stateService.fetchSessionIndexWithReadIndex(clientId)

                            .onSuccess(set->{
                                if (set.isEmpty()) {
                                    message.reply(new SessionResult().setLocal(false));
                                    return;
                                }
                                if (set.contains(mqttCluster.id())){
                                    message.reply(new SessionResult().setLocal(true));
                                }else {
                                    clusterClient.requestSession(clusterClient.nextRequestId(), new HashSet<>(set), clientId)
                                            .onFailure(t->message.fail(500,t.getMessage()))
                                            .onSuccess(sessionObj->{
                                                message.reply(new SessionResult().setLocal(false).setSessionObj(sessionObj));
                                            });
                                }
                            });
                });


        vertx.eventBus().<JsonArray>consumer("_retain_message_match_")
                .handler(message->{
                    JsonArray jsonArray = message.body();
                    stateService
                            .retainMapWithReadIndex()
                            .map(map->map.keySet()
                                    .stream()
                                    .filter(topic->jsonArray.stream().anyMatch(o-> TopicUtil.matches(o.toString(),topic)))
                                    .map(map::get)
                                    .filter(Objects::nonNull)
                                    .collect(Collectors.toSet()))
                            .map(stateService::fetchMessageIndex)
                            .onFailure(t->{
                                logger.error("fetch message failed",t);
                                message.fail(500,t.getMessage());
                            })
                            .onSuccess(map->{
                               message.reply(new RetainMatchResult().setMatchMap(map));
                            });
                });


        vertx.eventBus().<JsonObject>consumer("_message_request_")
                .handler(message->{
                    JsonObject body = message.body();
                    JsonArray nodeIds=body.getJsonArray("nodeIds");
                    String msgId = body.getString("id");
                    Set<String> set = nodeIds.stream().map(Object::toString).collect(Collectors.toSet());
                    clusterClient.requestMessage(clusterClient.nextRequestId(),set,msgId)
                        .onFailure(t->{
                            logger.error("request message failed",t);
                            message.fail(500,t.getMessage());
                        })
                        .onSuccess(message::reply);
                });

        vertx.eventBus().<String>consumer("_message_request_index")
                .handler(message->{
                    String msgId = message.body();
                    stateService.fetchMessageIndexWithReadIndex(Set.of(msgId))
                            .onSuccess(map->{
                                Set<String> set = map.get(id);
                                set.remove(mqttCluster.id());
                                if (set.isEmpty()){
                                    message.reply(null);
                                    return;
                                }
                                clusterClient.requestMessage(clusterClient.nextRequestId(),set,id)
                                        .onFailure(t->{
                                            logger.error("request message with index failed",t);
                                            message.fail(500,t.getMessage());
                                        })
                                        .onSuccess(message::reply);
                            });

                });

        vertx.eventBus().<String>consumer("_topic_match_")
                .handler(message->{
                    String topic = message.body();
                    stateService.topicMatchesWithReadIndex(topic)
                            .onFailure(t->{
                                message.fail(500,t.getMessage());
                            })
                            .onSuccess(c->{
                                Set<String> set = mqttCluster.net().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());
                                message.reply(new TopicMatchResult().setAllNodeIds(set).setSubscribeInfos(c));
                            });
                });

        vertx.eventBus().<UnSafeJsonObject>consumer("_send_message_")
                .handler(message->{
                    UnSafeJsonObject unSafeJsonObject = message.body();
                    JsonObject json = unSafeJsonObject.getJsonObject();
                    String nodeId = json.getString("nodeId");
                    JsonObject body = json.getJsonObject("body");
                    mqttCluster.net().request(nodeId, clusterClient.nextRequestId(), new ProMessage("/dispatcher", body).encode());
                });

        mqttCluster.start().setHandler(startFuture);
    }

    private Future<Buffer> requestSession(JsonObject body) {
        Promise<Buffer> promise=Promise.promise();
        vertx.eventBus().<Buffer>request("_request_session_handler_",body,ar->{
            if (ar.succeeded()){
                promise.complete(ar.result().body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

    private Future<Buffer> requestMessage(JsonObject body){
        Promise<Buffer> promise=Promise.promise();
        vertx.eventBus().<Buffer>request("_request_message_handler_",body,ar->{
            if (ar.succeeded()){
                promise.complete(ar.result().body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();

    }

    private Future<Boolean> takenOverSession(JsonObject body){
        vertx.eventBus().send("_taken_over_session_handler_", body);
        return Future.succeededFuture(true);
    }

    private Future<Boolean> dispatcherMsg(JsonObject body){
        body.remove("rpc-nodeId");
        body.remove("rpc-requestId");
        vertx.eventBus().send("_dispatcher_message_handler_", body);
        return Future.succeededFuture(true);
    }
    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {

    }
}