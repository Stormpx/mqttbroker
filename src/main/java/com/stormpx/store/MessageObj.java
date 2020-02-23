package com.stormpx.store;

import io.vertx.core.json.JsonObject;

import java.time.Instant;

public class MessageObj {
    private int rnf=0;

    private long timestamp= Instant.now().getEpochSecond();

    private JsonObject message;

    public MessageObj(JsonObject message) {
        this.message = message;
    }

    public JsonObject getMessage() {
        return message;
    }



    public int add(int rnf){
        this.rnf+=rnf;
        return this.rnf;
    }

    public int getRnf() {
        return rnf;
    }

    public long getTimestamp() {
        return timestamp;
    }
}
