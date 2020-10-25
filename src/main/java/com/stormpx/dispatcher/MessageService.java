package com.stormpx.dispatcher;

import com.stormpx.cluster.Cluster;
import com.stormpx.cluster.mqtt.ActionLog;
import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.TopicUtil;
import com.stormpx.store.MessageStore;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class MessageService {
    private final static Logger logger= LoggerFactory.getLogger(MessageService.class);

    private Vertx vertx;

    private MessageStore messageStore;

    private boolean isCluster;

    private Cluster cluster;

    private DispatcherContext dispatcherContext;

    private MessageHook messageHook;

    public MessageService setMessageHook(MessageHook messageHook) {
        this.messageHook = messageHook;
        return this;
    }

//    public void exchangeFormCluster(MessageContext messageContext){
//        String topic = messageContext.getMessage().getTopic();
//        boolean retain = messageContext.getMessage().isRetain();
//        cluster.topicMatch(topic)
//                .onSuccess(r->{
//                    Collection<TopicFilter.SubscribeMatchResult> c = r.getSubscribeMatchResults();
//                    if (logger.isDebugEnabled())
//                        logger.debug("match list :{}",c);
//                    Set<String> sendSet=new HashSet<>();
//
//                    for (TopicFilter.SubscribeMatchResult subscribeMatchResult : c) {
//                        Set<String> shareTopics= subscribeMatchResult.getAllMatchSubscribe()
//                                .stream()
//                                .filter(TopicFilter.Entry::isShare)
//                                .map(TopicFilter.Entry::getTopicFilterName)
//                                .collect(Collectors.toSet());
//
//                        messageContext.setShareTopics(shareTopics);
//
//
//                        String nodeId = subscribeMatchResult.getClientId();
//                        sendSet.add(nodeId);
//                        if (nodeId.equals(cluster.getId())){
//                            exchange(messageContext);
//                        }else {
//                            cluster.sendMessage(nodeId,json);
//                        }
//                    }
//                    if (retain){
//                        Set<String> nodeIds = r.getAllNodeIds();
//                        nodeIds.stream().filter(Predicate.not(sendSet::contains)).forEach(id->{
//                            cluster.sendMessage(id,message);
//                        });
//                    }
//                });
//    }


    public void exchange(MessageContext messageContext){
        String id = messageContext.getId();
        DispatcherMessage message = messageContext.getMessage();
        int qos = message.getQos().value();
        String topic = message.getTopic();
        boolean retain = message.isRetain();
        Buffer payloads = message.getPayload();
        Collection<TopicFilter.SubscribeMatchResult> collection = dispatcherContext.getTopicFilter().matches(topic);
        boolean match = !collection.isEmpty();
        if (match||retain) {
            if (qos > 0) {
                if (match || payloads.length() != 0) {
                    saveMessage(message);
                }
            }
            if (retain) {
                boolean del = payloads.length() == 0 || qos == 0;
                saveRetain(topic,del ? null : id);
            }

        }
        if (match) {
            collection.forEach(info->{

                Set<String> hitShareSet = info.getAllMatchSubscribe()
                        .stream()
                        .filter(TopicFilter.Entry::isShare)
                        .map(TopicFilter.Entry::getTopicFilterName)
                        .collect(Collectors.toSet());

                MessageContext context = new MessageContext(messageContext.getMessage()).setShareTopics(hitShareSet);

                sendTo(info.getClientId(),context);

            });
        }

    }

    protected void sendTo(String id,MessageContext messageContext){
        vertx.eventBus().send(id+"_mqtt_message_dispatcher",messageContext);
    }



    private Map<String,Integer> refMap =new HashMap<>();
    private Map<String,DispatcherMessage> messageMap=new HashMap<>();
    private Map<String,String> retainMap;





    public Future<Integer> modifyRefCnt(String id, int delta){
        return getRef(id)
                .map(i->{
                    int count=i==null?0:i;
                    count+=delta;
                    //no one used
                    if (count <= 0){
                        //del message
                        delMessage(id);
                        refMap.remove(id);
                    }else{
                        refMap.put(id,count);
                        messageStore.saveRef(id,count);
                    }
                    return count;
                });

    }



    public Future<DispatcherMessage> getMessage(String id){
        return getLocalMessage(id);

    }

    public Future<DispatcherMessage> getLocalMessage(String id){
        DispatcherMessage message = messageMap.get(id);
        if (message!=null){
            return Future.succeededFuture(message);
        }else {
            return getMessage0(id)
                    .onSuccess(msg->messageMap.computeIfAbsent(id,k->msg))
                    .onFailure(t->logger.error("get message by id: {} failed",id,t));

        }

    }

    public void matchRetainMessage(List<String> topics, Handler<DispatcherMessage> handler){
        messageStore.retainMap()
                .onFailure(t->logger.error("fetch retainMapWithReadIndex failed",t))
                .onSuccess(map->{
                    map.entrySet()
                            .stream()
                            .filter(e->topics.stream().anyMatch(o-> TopicUtil.matches(o,e.getKey())))
                            .filter(e->e.getValue()!=null)
                            .forEach(e-> getMessage(e.getValue()).onSuccess(dm->{
                                if (dm==null||dm.isExpiry()){
                                    //del retain
                                    saveRetain(e.getKey(),null);
                                } else{
                                    handler.handle(dm);
                                }
                            }));
                });
    }


    /**
     * try get ref from refmap
     * @param id
     * @return
     */
    private Future<Integer> getRef(String id){
        Integer ref = refMap.get(id);
        if (ref!=null){
            return messageStore.getRef(id)
                    .onFailure(t->logger.error("get message ref failed",t))
                    .onSuccess(i->refMap.computeIfAbsent(id,k->i));
        }else{
            return Future.succeededFuture(ref);
        }
    }

    protected Future<DispatcherMessage> getMessage0(String id){
        return messageStore.get(id);
    }

    private void saveMessage(DispatcherMessage dispatcherMessage){
        messageMap.put(dispatcherMessage.getId(),dispatcherMessage);
        messageStore.save(dispatcherMessage.getId(),dispatcherMessage);
        if (messageHook!=null)
            messageHook.onSaveMessage(dispatcherMessage);
    }

    private void delMessage(String id){
        messageMap.remove(id);
        messageStore.del(id);
        if (messageHook!=null)
            messageHook.onDelMessage(id);
    }

    private Future<Void> retainMap(){
        if (retainMap==null)
            return messageStore.retainMap().onSuccess(map->this.retainMap=map).map((Void)null);
        else
            return Future.succeededFuture();
    }

    private void saveRetain(String topic,String id){
        retainMap()
                .onSuccess(v->{
                    String oid = retainMap.get(topic);
                    retainMap.put(topic, id);
                    messageStore.putRetain(topic, id)
                            .onFailure(t->logger.error("save retain topic:{} id:{} failed",t,topic,id));
                    if (oid!=null&&!oid.equals(id)){
                        modifyRefCnt(oid,-1);
                    }
                    if (id!=null){
                        modifyRefCnt(id,1);
                    }
                });
    }


}
