package com.stormpx.dispatcher;

import com.stormpx.broker.MatchResultReducer;
import com.stormpx.broker.RetainMap;
import com.stormpx.kit.TopicFilter;
import com.stormpx.store.MessageStore;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class MessageService {
    protected final static Logger logger= LoggerFactory.getLogger(MessageService.class);


    protected MessageStore messageStore;


    protected DispatcherContext dispatcherContext;

    private MessageHook messageHook;

    public MessageService( MessageStore messageStore, DispatcherContext dispatcherContext) {
        this.messageStore = messageStore;
        this.dispatcherContext = dispatcherContext;
    }

    public MessageService setMessageHook(MessageHook messageHook) {
        this.messageHook = messageHook;
        return this;
    }


    public void dispatcher(MessageContext messageContext){
        String id = messageContext.getId();
        DispatcherMessage message = messageContext.getMessage();
        int qos = message.getQos().value();
        String topic = message.getTopic();
        boolean retain = message.isRetain();
        Buffer payloads = message.getPayload();

        // allowed shared subscriptions
        Set<String> shareTopics = messageContext.getShareTopics();

        Collection<TopicFilter.MatchResult> collection = dispatcherContext.getTopicFilter().matches(topic);
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

                MatchResultReducer reducer = new MatchResultReducer(info.getAllMatchSubscribe());
                reducer.reduce(shareTopics);

                MessageContext context = new MessageContext(messageContext.getMessage()).setShareTopics(reducer.getShareTopicName());

                sendTo(info.getClientId(),context);

            });
        }

    }

    protected void sendTo(String id,MessageContext messageContext){
        dispatcherContext.getVertx().eventBus().send(id+"_mqtt_message_dispatcher",messageContext);
    }


    public Future<DispatcherMessage> getMessage(String id){
        return getLocalMessage(id);

    }

    public Future<DispatcherMessage> getLocalMessage(String id){
        DispatcherMessage message = messageMap.get(id);
        if (message!=null){
            return Future.succeededFuture(message);
        }else {
            return messageStore.get(id)
                    .onSuccess(msg->messageMap.computeIfAbsent(id,k->msg))
                    .onFailure(t->logger.error("get message by id: {} failed",id,t));

        }

    }

    public void matchRetainMessage(List<String> topics, Handler<DispatcherMessage> handler){
        retainMap()
                .onSuccess(v->logger.debug("retainMap {}",retainMap))
                .onSuccess(v-> new RetainMap(this.retainMap)
                        .match(topics)
                        .forEach(sp-> {
                            logger.debug("match sp {}",sp);
                            sendLocalMessage(sp.getKey(),sp.getValue(),handler);
                        }));
    }

    protected void sendLocalMessage(String topic, String id, Handler<DispatcherMessage> handler){
        getLocalMessage(id).onSuccess(dm->{
            if (dm==null||dm.isExpiry()){
                //del retain
                saveRetain(topic,null);
            } else{
                handler.handle(dm);
            }
        });
    }

    private Map<String,Integer> refMap =new HashMap<>();
    private Map<String,DispatcherMessage> messageMap=new HashMap<>();
    private Promise<Void> retainMapPromise=Promise.promise();
    private Map<String,String> retainMap;




    public Future<Integer> modifyRefCnt(String id, int delta){
        return getRef(id)
                .map(i->{
                    int count=i==null?0:i;
                    count+=delta;
//                    logger.info("id :{} count:{}",id,count);
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

    /**
     * try get ref from refmap
     * @param id
     * @return
     */
    private Future<Integer> getRef(String id){
        Integer ref = refMap.get(id);
        if (ref==null){
            return messageStore.getRef(id)
                    .onFailure(t->logger.error("get message ref failed",t))
                    .onSuccess(i->refMap.computeIfAbsent(id,k->i));
        }else{
            return Future.succeededFuture(ref);
        }
    }


    protected void saveMessage(DispatcherMessage dispatcherMessage){
        if (messageMap.get(dispatcherMessage.getId())==null){
            messageMap.put(dispatcherMessage.getId(), dispatcherMessage);
            messageStore.save(dispatcherMessage.getId(), dispatcherMessage);
            if (messageHook != null)
                messageHook.onSaveMessage(dispatcherMessage);
        }
    }

    private void delMessage(String id){
        messageMap.remove(id);
        messageStore.del(id);
        if (messageHook!=null)
            messageHook.onDelMessage(id);
    }

    private Future<Void> retainMap(){
        if (retainMap==null){
            messageStore.retainMap()
                    .onSuccess(map->{
                        if (!retainMapPromise.future().isComplete()) {
                            this.retainMap = map;
                            retainMapPromise.complete();
                        }
                    });
        }
        return retainMapPromise.future();

    }

    protected void saveRetain(String topic,String id){
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
