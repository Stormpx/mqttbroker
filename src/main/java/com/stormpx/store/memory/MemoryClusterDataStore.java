package com.stormpx.store.memory;

import com.fasterxml.jackson.databind.util.JSONPObject;
import com.stormpx.cluster.LogEntry;
import com.stormpx.store.ClusterDataStore;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.TreeMap;

public class MemoryClusterDataStore implements ClusterDataStore {

    private JsonObject index;
    private JsonObject state;
    private int requestId;
    private TreeMap<Integer,LogEntry> logMap;

    public MemoryClusterDataStore() {
        this.logMap=new TreeMap<>();
    }

    @Override
    public void setRequestId(int requestId) {
        this.requestId=requestId;
    }

    @Override
    public Future<Integer> requestId() {
        return Future.succeededFuture(requestId);
    }

    @Override
    public void saveIndex(int firstIndex, int lastIndex) {
        if (index==null)
            index=new JsonObject();
        index.put("firstIndex",firstIndex);
        index.put("lastIndex",lastIndex);
    }

    @Override
    public Future<JsonObject> getIndex() {
        return Future.succeededFuture(index);
    }

    @Override
    public void saveState(JsonObject state) {
        this.state=state;
    }

    @Override
    public Future<JsonObject> getState() {
        return Future.succeededFuture(state);
    }

    @Override
    public Future<List<LogEntry>> logs() {
        return Future.succeededFuture(new ArrayList<>(logMap.values()));
    }

    @Override
    public Future<List<LogEntry>> getLogs(int start, int end) {
        return Future.succeededFuture(new ArrayList<>(logMap.subMap(start,end).values()));
    }

    @Override
    public void saveLog(LogEntry logEntry) {
        logMap.put(logEntry.getIndex(),logEntry);
    }

    @Override
    public void delLog(int start, int end) {
        while (start<end){
            logMap.remove(start++);
        }
    }
}
