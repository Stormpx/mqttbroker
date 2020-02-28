package com.stormpx.cluster.message;

import com.stormpx.kit.MqttCodecUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class ProMessage {
    private String res;
    private JsonObject body;

    public ProMessage(String res, JsonObject body) {
        this.res = res;
        this.body = body;
    }


    public static ProMessage decode(Buffer buffer){
        int pos=0;
        int len = buffer.getUnsignedShort(pos);
        pos+=2;
        Buffer res = buffer.getBuffer(pos, pos + len);
        pos+=len;
        JsonObject json = buffer.slice(pos, buffer.length()).toJsonObject();
        return new ProMessage(res.toString("utf-8"),json);

        /*RequestType requestType = RequestType.valueOf(buffer.getByte(0));
        JsonObject body = buffer.slice(0, buffer.length()).toJsonObject();
        return new ProMessage(requestType,body);*/
    }

    public ProMessage setRes(String res) {
        this.res = res;
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
        byte[] utf8String = MqttCodecUtil.encodeUtf8String(res);
        return Buffer.buffer(2+utf8String.length+buffer.length())
                .appendUnsignedShort(utf8String.length)
                .appendBytes(utf8String)
                .appendBuffer(buffer);
    }

    public String getRes() {
        return res;
    }
}
