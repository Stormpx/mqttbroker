package com.stormpx.message;

import io.vertx.core.net.impl.VertxHandler;

import java.util.List;
import java.util.Set;

public class MessageContext {

    private boolean unlimited;
    private Set<String> shareTopics;
    private MqttBrokerMessage message;


    public boolean isUnlimited() {
        return unlimited;
    }

    public MessageContext setUnlimited(boolean unlimited) {
        this.unlimited = unlimited;
        return this;
    }

    public Set<String> getShareTopics() {
        return shareTopics;
    }

    public MessageContext setShareTopics(Set<String> shareTopics) {
        this.shareTopics = shareTopics;
        return this;
    }

    public MqttBrokerMessage getMessage() {
        return message;
    }

    public MessageContext setMessage(MqttBrokerMessage message) {
        this.message = message;
        return this;
    }



}
