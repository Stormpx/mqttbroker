package com.stormpx.cluster;

import com.stormpx.Constants;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.mqtt.*;
import com.stormpx.dispatcher.api.Center;
import com.stormpx.kit.TopicUtil;
import com.stormpx.store.ClusterDataStore;
import com.stormpx.store.rocksdb.RocksDBClusterDataStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClusterVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);

    private ClusterDataStore clusterDataStore;

    private MqttCluster mqttCluster;
    private MqttStateService stateService;
    private ClusterClient clusterClient;

    private Center center;

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
        cluster.put(Constants.SAVE_DIR,saveDir);
        //cluster enable
        this.clusterDataStore=new RocksDBClusterDataStore(vertx,id);
        this.stateService=new MqttStateService(vertx);
        this.center =new Center(vertx);

        //            this.stateService.addHandler("/store/sync",this::);
        //            this.stateService.addHandler("/session/sync",);
        this.stateService.addHandler("/session",json->{
            String clientId = json.getString("clientId");
            return center.getSession(clientId);
        });
        this.stateService.addHandler("/message",json->{
            String mid = json.getString("id");
            return center.getMessage(mid);
        });
        this.stateService.addHandler("/takenover",center::takenOverSession);
        this.stateService.addHandler("/dispatcher",center::dispatcherMsg);
        this.stateService.reSetSessionHandler(center::resetSession);

        this.clusterClient=new ClusterClient(vertx,stateService,clusterDataStore);
        this.mqttCluster=new MqttCluster(vertx,cluster,clusterDataStore,stateService,clusterClient);
        logger.info("cluster enable id:{} port:{} nodes:{}",id,port,nodes);

        Cluster.Consumer consumer = new Cluster(vertx,id).consumer();
        consumer.sessionTakenoverHandler(body->{
            clusterClient.takenOverSession(body);
        }).proposalHandler(actionLog->{
            clusterClient.proposal(actionLog);
        }).sessionRequestHandler(message->{
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
        }).retainMessageMatchHandler(message->{
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
        }).messageRequestHandler(message->{
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
        }).messageRequestIndexHandler(message->{
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

        }).topicMatchHandler(message->{
            String topic = message.body();
            stateService.topicMatchesWithReadIndex(topic)
                    .onFailure(t->{
                        message.fail(500,t.getMessage());
                    })
                    .onSuccess(c->{
                        Set<String> set = mqttCluster.net().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet());
                        message.reply(new TopicMatchResult().setAllNodeIds(set).setSubscribeMatchResults(c));
                    });
        }).sendMessageHandler(message->{
            JsonObject json = unSafe.getJsonObject();
            String nodeId = json.getString("nodeId");
            JsonObject body = json.getJsonObject("body");
            mqttCluster.net().request(nodeId, new RpcMessage("/dispatcher",clusterClient.nextRequestId(), body).encode());
        });



        mqttCluster.start().setHandler(startFuture);
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {

    }
}
