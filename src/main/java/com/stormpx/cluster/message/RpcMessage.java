package com.stormpx.cluster.message;

import com.stormpx.kit.MqttCodecUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class RpcMessage {
    private String res;
    private int requestId;
    private JsonObject body;

    public RpcMessage(String res, int requestId, JsonObject body) {
        this.res = res;
        this.requestId = requestId;
        this.body = body;
    }

    public static RpcMessage decode(Buffer buffer){
        int pos=0;
        int len = buffer.getUnsignedShort(pos);
        pos+=2;
        int requestId = buffer.getInt(pos);
        pos+=4;
        Buffer res = buffer.getBuffer(pos, pos + len);
        pos+=len;
        JsonObject json = buffer.slice(pos, buffer.length()).toJsonObject();
        return new RpcMessage(res.toString("utf-8"),requestId,json);

        /*RequestType requestType = RequestType.valueOf(buffer.getByte(0));
        JsonObject body = buffer.slice(0, buffer.length()).toJsonObject();
        return new ProMessage(requestType,body);*/
    }

    public RpcMessage setRes(String res) {
        this.res = res;
        return this;
    }

    public JsonObject getBody() {
        return body;
    }

    public RpcMessage setBody(JsonObject body) {
        this.body = body;
        return this;
    }

    public Buffer encode(){
        Buffer buffer = body.toBuffer();
        byte[] utf8String = MqttCodecUtil.encodeUtf8String(res);
        return Buffer.buffer(2+utf8String.length+4+buffer.length())
                .appendUnsignedShort(utf8String.length)
                .appendInt(requestId)
                .appendBytes(utf8String)
                .appendBuffer(buffer);
    }

    public String getRes() {
        return res;
    }

    public int getRequestId() {
        return requestId;
    }
}
