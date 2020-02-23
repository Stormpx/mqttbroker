package com.stormpx.cluster.message;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class ProMessage {
    private RequestType requestType;
    private JsonObject body;


    public ProMessage(RequestType requestType, JsonObject body) {
        this.requestType = requestType;
        this.body = body;
    }

    public static ProMessage decode(Buffer buffer){
        RequestType requestType = RequestType.valueOf(buffer.getByte(0));
        JsonObject body = buffer.slice(0, buffer.length()).toJsonObject();
        return new ProMessage(requestType,body);
    }

    public RequestType getRequestType() {
        return requestType;
    }

    public ProMessage setRequestType(RequestType requestType) {
        this.requestType = requestType;
        return this;
    }

    public JsonObject getBody() {
        return body;
    }

    public ProMessage setBody(JsonObject body) {
        this.body = body;
        return this;
    }

    public Buffer encode(){
        Buffer buffer = body.toBuffer();
        return Buffer.buffer(1+buffer.length())
                .appendByte((byte) requestType.getValue())
                .appendBuffer(buffer);
    }

}
