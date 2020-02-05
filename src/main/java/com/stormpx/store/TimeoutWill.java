package com.stormpx.store;

import com.stormpx.kit.MqttCodecUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;

public class TimeoutWill {
    private String clientId;
    private long dispatcherTime;



    public TimeoutWill() {
    }

    public TimeoutWill(String clientId, long dispatcherTime) {
        this.clientId = clientId;
        this.dispatcherTime = dispatcherTime;
    }

    public static TimeoutWill fromJson(JsonObject jsonObject){
        return new TimeoutWill(jsonObject.getString("clientId"),jsonObject.getLong("dispatcherTime"));
    }

    public String getClientId() {
        return clientId;
    }

    public TimeoutWill setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }

    public long getDispatcherTime() {
        return dispatcherTime;
    }

    public TimeoutWill setDispatcherTime(long dispatcherTime) {
        this.dispatcherTime = dispatcherTime;
        return this;
    }

    public JsonObject toJson(){
        JsonObject json = new JsonObject();
        json.put("clientId",clientId);
        json.put("dispatcherTime",dispatcherTime);
        return json;
    }

    public Buffer toBuffer(){
        byte[] bytes = MqttCodecUtil.encodeUtf8String(clientId);
        Buffer buffer = Buffer.buffer(2 + bytes.length + 8);
        buffer.appendUnsignedShort(bytes.length)
                .appendBytes(bytes)
                .appendLong(dispatcherTime);
        return buffer;
    }


}
