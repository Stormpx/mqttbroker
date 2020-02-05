package com.stormpx.store;

import io.vertx.core.json.JsonObject;

public class SessionState {
    private String clientId;
    private Long expiryTimestamp;

    public SessionState(String clientId, Long expiryTimestamp) {
        this.clientId = clientId;
        this.expiryTimestamp = expiryTimestamp;
    }

    public static SessionState fromJson(JsonObject json){
        String clientId = json.getString("clientId");
        Long expiryTimestamp = json.getLong("expiryTimestamp");
        return new SessionState(clientId,expiryTimestamp);
    }

    public String getClientId() {
        return clientId;
    }

    public Long getExpiryTimestamp() {
        return expiryTimestamp;
    }


    public JsonObject toJson(){
        JsonObject json = new JsonObject();
        json.put("clientId",clientId);
        json.put("expiryTimestamp",expiryTimestamp);
        return json;

    }
}
