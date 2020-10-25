package com.stormpx.dispatcher.command.cluster;

public class ClusterCommand {

    public String from;
    private String to;


    public String getFrom() {
        return from;
    }

    public String getTo() {
        return to;
    }

    public ClusterCommand setFrom(String from) {
        this.from = from;
        return this;
    }

    public ClusterCommand setTo(String to) {
        this.to = to;
        return this;
    }
}
