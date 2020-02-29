package com.stormpx.store;

import com.stormpx.cluster.LogEntry;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface ClusterDataStore {

    void setRequestId(int requestId);

    Future<Integer> requestId();

    void saveIndex(int firstIndex,int lastIndex);

    Future<JsonObject> getIndex();

    void saveState(JsonObject state);

    Future<JsonObject> getState();

    Future<List<LogEntry>> logs();

    Future<List<LogEntry>> getLogs(int start, int end);

    void saveLog(LogEntry logEntry);

    void delLog(int start,int end);


}
