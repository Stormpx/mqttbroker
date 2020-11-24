package com.stormpx.cluster.mqtt;

import com.stormpx.broker.RetainMap;
import com.stormpx.dispatcher.api.Cluster;
import com.stormpx.cluster.ClusterNode;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.mqtt.command.RequestMessageCommand;
import com.stormpx.cluster.mqtt.command.SendMessageCommand;
import com.stormpx.dispatcher.command.TakenOverCommand;
import com.stormpx.kit.Codec;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClusterApiController {
    private final static Logger logger= LoggerFactory.getLogger(ClusterApiController.class);

    private Vertx vertx;

    private ClusterClient clusterClient;
    private MqttStateService stateService;
    private MqttCluster mqttCluster;

    public ClusterApiController(Vertx vertx, ClusterClient clusterClient, MqttStateService stateService, MqttCluster mqttCluster) {
        this.vertx = vertx;
        this.clusterClient = clusterClient;
        this.stateService = stateService;
        this.mqttCluster = mqttCluster;
        initApi();
    }

    private void initApi(){

        EventBus eventBus = vertx.eventBus();


        eventBus.<TakenOverCommand>consumer("_mqtt_session_taken_over")
                .handler(message->{
                    TakenOverCommand command = message.body();
                    String clientId = command.getClientId();
                    if (command.isLocalOnly())
                        return;
                    clusterClient.takenOverSession(new JsonObject().put("clientId",clientId).put("sessionEnd",command.isSessionEnd()));
                });

        eventBus.<ActionLog>localConsumer(Cluster.LOG_PROPOSAL)
                .handler(msg->{
                    ActionLog actionLog = msg.body();
                    clusterClient.proposal(actionLog);
                });
        eventBus.<String>localConsumer(Cluster.SESSION_REQUEST)
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
                                            .onSuccess(session->{
                                                message.reply(new SessionResult().setLocal(false).setSession(session));
                                            });
                                }
                            });
                });

        eventBus.<JsonArray>localConsumer(Cluster.RETAIN_MESSAGE_MATCH)
                .handler(message->{
                    JsonArray jsonArray = message.body();
                    stateService
                            .retainMapWithReadIndex()
                            .map(map-> new RetainMap(map).match(jsonArray.stream().map(Object::toString).collect(Collectors.toList())))
                            .map(list->{
                                RetainMatchResult result = new RetainMatchResult();
                                list.forEach(sp->{
                                    Set<String> nodeSet = stateService.fetchMessageIndex(sp.getValue());
                                    result.addMatch(sp.getKey(),sp.getValue(),nodeSet);
                                });
                                return result;
                            })
                            .onFailure(t->logger.error("fetch message failed",t))
                            .onFailure(t->message.fail(500,t.getMessage()))
                            .onSuccess(message::reply)

                    ;
                });


        eventBus.<RequestMessageCommand>localConsumer(Cluster.MESSAGE_REQUEST)
                .handler(message->{
                    RequestMessageCommand command = message.body();
                    String messageId = command.getId();
                    getNodeIds(command)
                            .compose(set->clusterClient.requestMessage(clusterClient.nextRequestId(),set,messageId))
                            .onFailure(t->{
                                logger.error("request message failed",t);
                                message.fail(500,t.getMessage());
                            })
                            .onSuccess(message::reply);
                });


        eventBus.<String>localConsumer(Cluster.TOPIC_MATCH)
                .handler(message->{
                    String topic = message.body();
                    stateService.topicMatchesWithReadIndex(topic)
                            .onFailure(t-> message.fail(500,t.getMessage()))
                            .onSuccess(c-> message.reply(new TopicMatchResult()
                                    .setAllNodeIds(mqttCluster.net().nodes().stream().map(ClusterNode::id).collect(Collectors.toSet()))
                                    .setMatchResults(c)));
                });

        eventBus.<SendMessageCommand>localConsumer(Cluster.SEND_MESSAGE)
                .handler(message->{
                    SendMessageCommand command = message.body();
                    String nodeId = command.getNodeId();
                    var json=new JsonObject().put("share",command.getShareTopics())
                            .put("message", Codec.encode(command.getMessage()));
                    mqttCluster.net().request(nodeId, new RpcMessage("/dispatcher",clusterClient.nextRequestId(), json.toBuffer()).encode());
                });
    }

    private Future<Set<String>> getNodeIds(RequestMessageCommand command){
        if (command.getNodeIds()==null||command.getNodeIds().isEmpty()){
            return stateService.fetchMessageIndexWithReadIndex(Set.of(command.getId()))
                    .map(map->map.get(command.getId()));

        }
        return Future.succeededFuture(command.getNodeIds());

    }
}
