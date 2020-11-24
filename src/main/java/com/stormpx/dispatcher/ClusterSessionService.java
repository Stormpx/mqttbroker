package com.stormpx.dispatcher;

import com.stormpx.dispatcher.api.Cluster;
import com.stormpx.cluster.mqtt.ActionLog;
import com.stormpx.dispatcher.command.CloseSessionCommand;
import com.stormpx.dispatcher.command.SubscriptionsCommand;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;
import com.stormpx.store.SessionStore;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ClusterSessionService extends SessionService {

    private Cluster cluster;

    public ClusterSessionService(SessionStore sessionStore, DispatcherContext dispatcherContext,Cluster cluster) {
        super(sessionStore, dispatcherContext);
        this.cluster=cluster;
    }

    @Override
    protected Future<Boolean> getOrCreateSession(String id,String clientId, boolean cleanSession) {

        return cluster.requestSession(clientId)
                .compose(r->{
                    if (r.isLocal()){
                        return super.getOrCreateSession(id,clientId, cleanSession);
                    }else {
                        ClientSession session = r.getSession();
                        if (ClientSession.isExpiry(session.getExpiryTimestamp())||cleanSession){

                            cluster.proposal(ActionLog.saveSession(cluster.getId(),clientId,cleanSession));
                            //publish will
                            if (session.getWill()!=null){
                                dispatcherContext.getMessageService().dispatcher(new MessageContext(session.getWill()));
                            }

                            return Future.succeededFuture(true);
                        }else{


                            //request message to recover message ref
                            return recoverLink(session.getMessageLinks())
                                    .compose(v->sessionStore.save(session))
                                    .compose(v->recoverSubscription(id,clientId,session.getTopicSubscriptions()))
                                    .onSuccess(v->cluster.proposal(ActionLog.saveSession(cluster.getId(),clientId, false)))
                                    .map(v->false);


                        }
                    }
                });
    }

    private Future<Void> recoverLink(List<MessageLink> links){
        MessageService messageService = dispatcherContext.getMessageService();
        List<Future> list=links.stream()
                .filter(Predicate.not(MessageLink::isDiscard))
                .map(link-> messageService.getMessage(link.getId())
                        .onSuccess(messageService::saveMessage)
                        .compose(message-> messageService.modifyRefCnt(link.getId(),1))
                )
                .collect(Collectors.toList())
                ;
        return CompositeFuture.all(list).map(v->null);
    }

    private Future<Void> recoverSubscription(String id,String clientId,List<MqttSubscription> subscriptions){
        if (!subscriptions.isEmpty()){
            dispatcherContext.getSubscriptionService()
                    .subscribe(new SubscriptionsCommand()
                            .setRecover(true)
                            .setId(id)
                            .setMqttSubscriptions(subscriptions)
                    );
        }
        return Future.succeededFuture();
    }

    @Override
    public Future<Void> delSession(String clientId) {
        cluster.proposal(ActionLog.delSession(cluster.getId(),clientId));
        return super.delSession(clientId);
    }





    @Override
    public void closeSession(CloseSessionCommand closeSessionCommand) {
        super.closeSession(closeSessionCommand);
    }
}
