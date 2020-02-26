package com.stormpx.store;

import io.vertx.core.json.JsonObject;

import java.time.Instant;

public class MessageObj {
    private int refCnt =0;

    private long timestamp= Instant.now().getEpochSecond();

    private JsonObject message;

    public MessageObj(JsonObject message) {
        this.message = message;
    }

    public JsonObject getMessage() {
        return message;
    }



    public int add(int rnf){
        this.refCnt +=rnf;
        return this.refCnt;
    }

    public int getRefCnt() {
        return refCnt;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "MessageObj{" + "refCnt=" + refCnt + ", message=" + message + '}';
    }
}
