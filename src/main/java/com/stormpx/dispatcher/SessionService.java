package com.stormpx.dispatcher;

import com.stormpx.dispatcher.command.ClientAcceptCommand;
import com.stormpx.dispatcher.command.CloseSessionCommand;
import com.stormpx.dispatcher.command.PacketIdActionCommand;
import com.stormpx.dispatcher.command.TakenOverCommand;
import com.stormpx.kit.F;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;
import com.stormpx.store.SessionStore;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.TimeoutStream;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class SessionService {
    private final static Logger logger= LoggerFactory.getLogger(SessionService.class);

    protected SessionStore sessionStore;

    private Map<String, TimeoutStream> willTimerMap=new HashMap<>();

    protected DispatcherContext dispatcherContext;


    public SessionService(SessionStore sessionStore, DispatcherContext dispatcherContext) {
        this.sessionStore = sessionStore;
        this.dispatcherContext = dispatcherContext;
    }


    protected Future<Boolean> getOrCreateSession(String id,String clientId,boolean cleanSession){
        if (cleanSession){
            // publish will immediately
            //if session still connecting sessionStore.getWill() result may be null
            return sessionStore.getWill(clientId)
                    .onSuccess(msg->{
                        if (msg!=null)
                            dispatcherContext.getMessageService().dispatcher(new MessageContext(msg));
                    })
                    .compose(v->delSession(clientId))
                    .map(true);
        }else{
            return sessionStore.getExpiryTimestamp(clientId)
                    .map(ClientSession::isExpiry)
                    .onSuccess(b->{
                        if (b)
                            delSession(clientId);
                    });

        }

    }


    /**
     *
     * @param command
     * @return
     */
    public Future<Boolean> acceptSession(ClientAcceptCommand command){
        takenOverSession(command.getClientId(),command.getSessionId(),command.isCleanSession());
        return getOrCreateSession(command.getId(),command.getClientId(),command.isCleanSession())
                .onSuccess(expiry->{
                   if (!expiry){
                       stopWillTimer(command.getClientId());
                       sessionStore.links(command.getClientId())
                               .onSuccess(links->reLink(links,command.getAddress()));
                       sessionStore.packetId(command.getAddress())
                                .onSuccess(packetIds-> callClient(command.getAddress(),EventContext.packetIds(PacketIds.unConfirmId(new HashSet<>(packetIds)))));
                       sessionStore.getSubscription(command.getClientId())
                                .onSuccess(s->callClient(command.getAddress(),EventContext.subscription(s)));

                   }
                })
                .onFailure(t->logger.error("accept client :{} failed",t,command.getClientId()));
    }




    public void callClient(String address,EventContext event){
        dispatcherContext.getVertx().eventBus().send(address,event);
    }


    private void reLink(List<MessageLink> links,String callbackAddress){
        Set<Integer> set=new HashSet<>();
        List<MessageLink> messageId=new ArrayList<>();
        for (MessageLink link : links) {
            if (link.isDiscard()) {
                // packetId
                set.add(link.getPacketId());
            } else {
                //message
                messageId.add(link);
            }
        }
        if (!set.isEmpty()){
            callClient(callbackAddress,EventContext.packetIds(PacketIds.unReceivedId(set)));
        }
        if (!messageId.isEmpty()){
            messageId.forEach(link->
                    dispatcherContext.getMessageService()
                            .getMessage(link.getId())
                            .compose(F::failWhenNull)
                            .map(dm->new MessageContext(dm.setQos(link.getQos()).setDup(true))
                                    .setSubscriptionIds(link.getSubscriptionId())
                                    .setPacketId(link.getPacketId()))
                            .onSuccess(messageContext-> callClient(callbackAddress,EventContext.message(messageContext)))
            );
            ;
        }
    }

    private void takenOverSession(String clientId,String id,boolean cleanSession){
        TakenOverCommand command = new TakenOverCommand(clientId, id, cleanSession,false);
        dispatcherContext.getVertx().eventBus().publish("_mqtt_session_taken_over",command);
    }

    public void takenOverLocalSession(String clientId,String id,boolean cleanSession){
        TakenOverCommand command = new TakenOverCommand(clientId, id, cleanSession,true);
        dispatcherContext.getVertx().eventBus().publish("_mqtt_session_taken_over",command);
    }



    private void stopWillTimer(String clientId){
        TimeoutStream timeoutStream = willTimerMap.remove(clientId);
        if (timeoutStream!=null)
            timeoutStream.cancel();
    }


    public Future<ClientSession> getSession(String clientId){
        return sessionStore.get(clientId);
    }


    /**
     *
     * @param clientId
     * @return
     */
    public Future<Void> delSession(String clientId){
        //setExpiryTimestamp to zero
        //guarantees that the next connection will be considered expired even if deletion fails.
        return sessionStore.setExpiryTimestamp(clientId,0L)
                .compose(v->sessionStore.links(clientId))
                .compose(this::releaseLink)
                .compose(v->sessionStore.del(clientId));
    }

    private Future<Void> releaseLink(List<MessageLink> links){
        List<Future> list=links.stream()
                .filter(link->!link.isDiscard())
                .map(link->dispatcherContext.getMessageService().modifyRefCnt(link.getId(),-1))
                .collect(Collectors.toList())
        ;
        return CompositeFuture.all(list).map(v->null);
    }



    public void link(MessageLink messageLink){
        if (messageLink.isOffLink()){
            sessionStore.addOfflineLink(messageLink.getClientId(),messageLink)
                .onFailure(t->logger.error("add offline link failed clientId:{} link:{}" ,t,messageLink.getClientId(),messageLink));
        }else {
            sessionStore.addLink(messageLink.getClientId(), messageLink)
                    .onFailure(t->logger.error("add link failed clientId:{} link:{}" ,t,messageLink.getClientId(),messageLink));
        }
    }


    public Future<String> packetId(PacketIdActionCommand packetIdActionCommand){
        Promise<String> promise=Promise.promise();
        if (packetIdActionCommand.isDiscard()){
            sessionStore.receive(packetIdActionCommand.getClientId(), packetIdActionCommand.getPacketId()).onComplete(promise);
        }else if (packetIdActionCommand.isRelease()){
            sessionStore.release(packetIdActionCommand.getClientId(), packetIdActionCommand.getPacketId()).onComplete(promise);
        }else if (packetIdActionCommand.isAddPacketId()){
            sessionStore.addPacketId(packetIdActionCommand.getClientId(), packetIdActionCommand.getPacketId()).map((String)null).onComplete(promise);
        }else if (packetIdActionCommand.isRemovePacketId()){
            sessionStore.removePacketId(packetIdActionCommand.getClientId(), packetIdActionCommand.getPacketId()).map((String)null).onComplete(promise);
        }
        return promise.future().onFailure(t->logger.error("packetId action :{} failed",t, packetIdActionCommand));
    }

    public Future<Void> saveSubscription(String clientId, List<MqttSubscription> subscriptions){
        return sessionStore.addSubscription(clientId, subscriptions);
    }

    public Future<Void> delSubscription(String clientId, List<String> topics){
        return sessionStore.deleteSubscription(clientId,topics);
    }



    /**
     * on mqtt connection close not consider taken over
     * @param closeSessionCommand
     */
    public void closeSession(CloseSessionCommand closeSessionCommand){
        String clientId = closeSessionCommand.getClientId();
        if (!closeSessionCommand.isTakenOver()) {
            if (closeSessionCommand.getSessionExpiryInterval() > 0) {
                sessionStore.setExpiryTimestamp(clientId, closeSessionCommand.getSessionExpiryInterval());
            } else  {
                //todo del
                delSession(clientId);
            }
        }
        logger.info("client connection:{} closed disconnect:{} sessionExpiry:{} will:{} willInterval:{}", clientId,closeSessionCommand.isDisconnect(),
                closeSessionCommand.getSessionExpiryInterval(),closeSessionCommand.isWill(),closeSessionCommand.getWillDelayInterval());

        if (closeSessionCommand.isDisconnect())
            return;

        if (closeSessionCommand.isWill()){
            long willDelayInterval = Math.min(closeSessionCommand.getWillDelayInterval(),closeSessionCommand.getSessionExpiryInterval());
            if (willDelayInterval<=0){
                // publish will
                logger.info("publish client:{} will",clientId);
                dispatcherContext.getMessageService().dispatcher(new MessageContext(closeSessionCommand.getWillMessage().copy()));
            }else if (!closeSessionCommand.isTakenOver()){

                var timeout=dispatcherContext.getVertx().timerStream(willDelayInterval)
                    .handler(id->{
                        willTimerMap.remove(clientId);
                        sessionStore.delWill(clientId);
                        logger.info("publish client:{} will",clientId);
                        dispatcherContext.getMessageService().dispatcher(new MessageContext(closeSessionCommand.getWillMessage().copy()));
                    });
                //save will
                sessionStore.saveWill(clientId,closeSessionCommand.getWillMessage());
                //stop  timer
                stopWillTimer(clientId);
                willTimerMap.put(clientId,timeout);
            }

        }
    }


}
