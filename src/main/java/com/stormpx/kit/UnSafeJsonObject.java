package com.stormpx.kit;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class UnSafeJsonObject {
    public final static UnSafeJsonObjectCodec CODEC=new UnSafeJsonObjectCodec();
    private JsonObject jsonObject;

    private UnSafeJsonObject(JsonObject jsonObject) {
        this.jsonObject = jsonObject;
    }

    public static UnSafeJsonObject wrapper(JsonObject jsonObject){
        return new UnSafeJsonObject(jsonObject);
    }

    public JsonObject getJsonObject() {
        return jsonObject;
    }


    private static class UnSafeJsonObjectCodec implements MessageCodec<UnSafeJsonObject,UnSafeJsonObject>{

        @Override
        public void encodeToWire(Buffer buffer, UnSafeJsonObject unSafeJsonObject) {
            JsonObject jsonObject = unSafeJsonObject.getJsonObject();
            Buffer encoded = jsonObject.toBuffer();
            buffer.appendInt(encoded.length());
            buffer.appendBuffer(encoded);
        }

        @Override
        public UnSafeJsonObject decodeFromWire(int pos, Buffer buffer) {
            int length = buffer.getInt(pos);
            pos += 4;
            return UnSafeJsonObject.wrapper(new JsonObject(buffer.slice(pos, pos + length)));
        }

        @Override
        public UnSafeJsonObject transform(UnSafeJsonObject unSafeJsonObject) {
            return unSafeJsonObject;
        }

        @Override
        public String name() {
            return "unSafeJsonObject";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
