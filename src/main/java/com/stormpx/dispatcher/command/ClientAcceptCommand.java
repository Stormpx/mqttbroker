package com.stormpx.dispatcher.command;

public class ClientAcceptCommand {

    private String clientId;
    private boolean cleanSession;
    private String id;
    private String sessionId;
    private String address;


    public static ClientAcceptCommand create(String clientId, boolean cleanSession, String id,String sessionId, String address){
        return new ClientAcceptCommand().setClientId(clientId).setCleanSession(cleanSession)
                .setId(id).setSessionId(sessionId).setAddress(address);
    }

    public String getClientId() {
        return clientId;
    }

    public ClientAcceptCommand setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public boolean isCleanSession() {
        return cleanSession;
    }

    public ClientAcceptCommand setCleanSession(boolean cleanSession) {
        this.cleanSession = cleanSession;
        return this;
    }

    public String getId() {
        return id;
    }

    public ClientAcceptCommand setId(String id) {
        this.id = id;
        return this;
    }

    public String getAddress() {
        return address;
    }

    public ClientAcceptCommand setAddress(String address) {
        this.address = address;
        return this;
    }

    public String getSessionId() {
        return sessionId;
    }

    public ClientAcceptCommand setSessionId(String sessionId) {
        this.sessionId = sessionId;
        return this;
    }
}
