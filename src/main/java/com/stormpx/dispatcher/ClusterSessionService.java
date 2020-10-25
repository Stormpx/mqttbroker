package com.stormpx.dispatcher;

import com.stormpx.cluster.Cluster;
import com.stormpx.dispatcher.command.ClientAcceptCommand;
import com.stormpx.dispatcher.command.CloseSessionCommand;
import com.stormpx.dispatcher.command.PacketIdActionCommand;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.MessageLink;
import com.stormpx.store.SessionStore;
import io.vertx.core.Future;

import java.util.List;

public class ClusterSessionService extends SessionService {
    private Cluster cluster;
    public ClusterSessionService(SessionStore sessionStore, DispatcherContext dispatcherContext,String id) {
        super(sessionStore, dispatcherContext);
        this.cluster=new Cluster(dispatcherContext.getVertx(),id);
    }

    @Override
    protected Future<Boolean> getOrCreateSession(String clientId, boolean cleanSession) {
        return super.getOrCreateSession(clientId, cleanSession);
    }

    @Override
    public Future<Boolean> acceptSession(ClientAcceptCommand command) {
        return super.acceptSession(command);
    }

    @Override
    public void callClient(String address, EventContext event) {
        super.callClient(address, event);
    }

    @Override
    protected void takenOverSession(String clientId, String id, boolean cleanSession) {
        super.takenOverSession(clientId, id, cleanSession);
    }

    @Override
    public void stopWillTimer(String clientId) {
        super.stopWillTimer(clientId);
    }

    @Override
    public Future<Void> delSession(String clientId) {
        return super.delSession(clientId);
    }

    @Override
    public void link(MessageLink messageLink) {
        super.link(messageLink);
    }

    @Override
    public Future<String> packetId(PacketIdActionCommand packetIdActionCommand) {
        return super.packetId(packetIdActionCommand);
    }

    @Override
    public Future<Void> saveSubscription(String clientId, List<MqttSubscription> subscriptions) {
        return super.saveSubscription(clientId, subscriptions);
    }

    @Override
    public Future<Void> delSubscription(String clientId, List<String> topics) {
        return super.delSubscription(clientId, topics);
    }

    @Override
    public void closeSession(CloseSessionCommand closeSessionCommand) {
        super.closeSession(closeSessionCommand);
    }
}
