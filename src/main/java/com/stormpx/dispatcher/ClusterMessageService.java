package com.stormpx.dispatcher;

import com.stormpx.broker.MatchResultReducer;
import com.stormpx.dispatcher.api.Cluster;
import com.stormpx.cluster.mqtt.ActionLog;
import com.stormpx.cluster.mqtt.RetainMatchResult;
import com.stormpx.cluster.mqtt.command.RequestMessageCommand;
import com.stormpx.cluster.mqtt.command.SendMessageCommand;
import com.stormpx.kit.F;
import com.stormpx.kit.TopicFilter;
import com.stormpx.store.MessageStore;
import io.vertx.core.Future;
import io.vertx.core.Handler;

import java.util.*;

public class ClusterMessageService extends MessageService {

    private Cluster cluster;

    public ClusterMessageService( MessageStore messageStore, DispatcherContext dispatcherContext, Cluster cluster) {
        super( messageStore, dispatcherContext);
        this.cluster = cluster;
        setMessageHook(new MessageHook() {
            @Override
            public void onSaveMessage(DispatcherMessage message) {
                cluster.proposal(ActionLog.saveMessage(cluster.getId(),message.getId(),message.isRetain(),message.getTopic(),message.getPayload().length()));
            }

            @Override
            public void onDelMessage(String id) {
                cluster.proposal(ActionLog.delMessage(cluster.getId(),id));
            }
        });
    }

    @Override
    public void dispatcher(MessageContext messageContext) {
        if (messageContext.isFromCluster()){
            super.dispatcher(messageContext);
            return;
        }
        DispatcherMessage message = messageContext.getMessage();
        String topic = message.getTopic();
        boolean retain = message.isRetain();
        cluster.topicMatch(topic)
                .onSuccess(r->{
                    Collection<TopicFilter.MatchResult> c = r.getMatchResults();

                    if (logger.isDebugEnabled())
                        logger.debug("match list :{}",c);

                    Set<String> sendSet=new HashSet<>(r.getAllNodeIds());

                    for (TopicFilter.MatchResult result : c) {

                        MatchResultReducer reducer = new MatchResultReducer(result.getAllMatchSubscribe())
                                .reduce(Collections.emptySet());

                        String nodeId = result.getClientId();
                        if (nodeId.equals(cluster.getId())){
                            messageContext.setShareTopics(reducer.getShareTopicName());
                            // local exchange
                            super.dispatcher(messageContext);

                        }else {

                            cluster.sendMessage(new SendMessageCommand(nodeId,message.copy()).setShareTopics(reducer.getShareTopicName()));

                        }
                        sendSet.remove(nodeId);
                    }
                    if (retain){
                        sendSet.forEach(id->cluster.sendMessage(new SendMessageCommand(id,message.copy())));
                    }
                });
    }

    @Override
    public void matchRetainMessage(List<String> topics, Handler<DispatcherMessage> handler) {
        cluster.retainMatch(topics)
                .onFailure(t->logger.error("fetch retainMapWithReadIndex failed",t))
                .onSuccess(retainMatchResult->{
                    Map<String, RetainMatchResult.MatchMessageIndex> map = retainMatchResult.getMatchMap();
                    if (map.isEmpty()) {
                        return;
                    }

                    map.forEach((topic, match) ->{
                        if (match.getNodeIds().contains(cluster.getId())){
                            sendLocalMessage(topic,match.getId(),handler);
                        }else{
                            cluster.requestMessage(new RequestMessageCommand(match.getId()).setNodeIds(match.getNodeIds()))
                                    .onFailure(t->logger.warn("get message failed msg:{}",t.getMessage()))
                                    .map(message->message.isExpiry()?null:message)
                                    .compose(F::failWhenNull)
                                    .onSuccess(message->{
                                        saveMessage(message);
                                        saveRetain(topic,match.getId());
                                        handler.handle(message);
                                    })
                            ;
                        }
                    });

                });
    }


    @Override
    public Future<DispatcherMessage> getMessage(String id) {
        return super.getMessage(id)
                .compose(F::failWhenNull)
                .recover(t->cluster.requestMessage(new RequestMessageCommand(id)));
    }
}
