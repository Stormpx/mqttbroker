package com.stormpx.cluster.mqtt.command;

import java.util.Set;

public class RequestMessageCommand {
    private Set<String> nodeIds;
    private String id;

    public RequestMessageCommand(String id) {
        this.id = id;
    }

    public Set<String> getNodeIds() {
        return nodeIds;
    }

    public RequestMessageCommand setNodeIds(Set<String> nodeIds) {
        this.nodeIds = nodeIds;
        return this;
    }

    public String getId() {
        return id;
    }

}
