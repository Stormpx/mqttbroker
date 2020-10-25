package com.stormpx.kit;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class UnSafeJsonObject {
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


}
