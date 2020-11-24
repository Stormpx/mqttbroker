package com.stormpx.store;

import com.stormpx.dispatcher.DispatcherMessage;
import io.vertx.core.Future;

import java.util.Map;

public interface MessageStore {

    Future<Map<String,String>> retainMap();

    Future<DispatcherMessage> get(String id);

    void save(String id, DispatcherMessage message);

    Future<Void> del(String id);

    Future<String> putRetain(String topic,String id);

    Future<Integer> getRef(String id);

    void saveRef(String id,int d);
}
