package com.stormpx.dispatcher.command;

public class TakenOverCommand {
    private String clientId;
    //new client connect id
    private String id;
    private boolean localOnly;

    private boolean sessionEnd;

    public TakenOverCommand(String clientId, String id,  boolean sessionEnd,boolean local) {
        this.clientId = clientId;
        this.id = id;
        this.localOnly = local;
        this.sessionEnd = sessionEnd;
    }

    public String getClientId() {
        return clientId;
    }

    public String getId() {
        return id;
    }

    public boolean isSessionEnd() {
        return sessionEnd;
    }

    public boolean isLocalOnly() {
        return localOnly;
    }
}
