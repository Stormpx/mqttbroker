package com.stormpx.store;

import io.vertx.core.Future;

import java.util.Map;

public interface MessageStore {

    Future<Map<String,String>> retainMap();

    Future<MessageObj> get(String id);

    void set(String id, MessageObj messageObj);

    void putRetain(String topic,String id);
}
