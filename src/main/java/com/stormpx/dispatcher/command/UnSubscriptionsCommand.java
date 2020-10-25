package com.stormpx.dispatcher.command;

import java.util.List;

public class UnSubscriptionsCommand {
    private String id;
    private String clientId;
    private List<String> topics;
    private List<String> verticleUnSubscribeTopic;

    public String getId() {
        return id;
    }

    public UnSubscriptionsCommand setId(String id) {
        this.id = id;
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public UnSubscriptionsCommand setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }


    public List<String> getTopics() {
        return topics;
    }

    public UnSubscriptionsCommand setTopics(List<String> topics) {
        this.topics = topics;
        return this;
    }

    public List<String> getVerticleUnSubscribeTopic() {
        return verticleUnSubscribeTopic;
    }

    public UnSubscriptionsCommand setVerticleUnSubscribeTopic(List<String> verticleUnSubscribeTopic) {
        this.verticleUnSubscribeTopic = verticleUnSubscribeTopic;
        return this;
    }
}
