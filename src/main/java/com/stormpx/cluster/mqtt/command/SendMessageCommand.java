package com.stormpx.cluster.mqtt.command;

import com.stormpx.dispatcher.DispatcherMessage;

import java.util.Set;

public class SendMessageCommand {
    private Set<String> nodeIds;
    private DispatcherMessage message;

    public SendMessageCommand(Set<String> nodeIds, DispatcherMessage message) {
        this.nodeIds = nodeIds;
        this.message = message;
    }

    public Set<String> getNodeIds() {
        return nodeIds;
    }

    public DispatcherMessage getMessage() {
        return message;
    }
}
