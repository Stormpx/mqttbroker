package com.stormpx.cluster.mqtt.command;

import com.stormpx.dispatcher.DispatcherMessage;

import java.util.Set;

public class SendMessageCommand {
    private String nodeId;
    private Set<String> shareTopics;
    private DispatcherMessage message;

    public SendMessageCommand(String nodeId, DispatcherMessage message) {
        this.nodeId = nodeId;
        this.message = message;
    }

    public String getNodeId() {
        return nodeId;
    }

    public Set<String> getShareTopics() {
        return shareTopics;
    }

    public SendMessageCommand setShareTopics(Set<String> shareTopics) {
        this.shareTopics = shareTopics;
        return this;
    }

    public DispatcherMessage getMessage() {
        return message;
    }
}
