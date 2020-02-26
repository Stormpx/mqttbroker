package com.stormpx.store;

import io.vertx.core.Future;

import java.util.Map;

public interface MessageStore {

    Future<Map<String,String>> retainMap();

    Future<MessageObj> get(String id);

    void set(String id, MessageObj messageObj);

    void del(String id);

    Future<String> putRetain(String topic,String id);

    Future<Integer> addAndGetRefCnt(String id,int d);
}
