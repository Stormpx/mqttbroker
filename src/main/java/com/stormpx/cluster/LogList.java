package com.stormpx.cluster;

import com.stormpx.store.ClusterDataStore;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

import java.util.*;

public class LogList {
    private ClusterDataStore clusterDataStore;
    private int firstLogIndex;
    private int lastLogIndex;

    private TreeMap<Integer,LogEntry> logEntryTreeMap;

    public LogList(ClusterDataStore clusterDataStore, int firstLogIndex, int lastLogIndex) {
        this.clusterDataStore = clusterDataStore;
        this.firstLogIndex = firstLogIndex;
        this.lastLogIndex = lastLogIndex;
        this.logEntryTreeMap=new TreeMap<>();
    }


    public Future<List<LogEntry>> getLog(int start, int end){
        if (start>end)
            return Future.succeededFuture(new ArrayList<>());
        if (start<1)
            start=1;
        if (start>lastLogIndex)
            return Future.succeededFuture(new ArrayList<>());

        if (start>=listFirstIndex()){
            Collection<LogEntry> entries = logEntryTreeMap.subMap(start , end).values();
            return Future.succeededFuture(new ArrayList<>(entries));
        }else {
            return clusterDataStore.getLogs(start, end)
                    .map(list -> {
                        if (list.isEmpty()) return list;

                        if (end>=listFirstIndex()){
                            for (LogEntry logEntry : list) {
                                logEntryTreeMap.put(logEntry.getIndex(),logEntry);
                            }
                        }

                        return list;
                    });
        }
    }

    public Future<LogEntry> getLog(int index){
        if (index<listFirstIndex()){
            //db
            return clusterDataStore.getLogs(index,index+1).map(list->{
                if (list.isEmpty())
                    return null;
                LogEntry logEntry = list.get(0);
                if (listFirstIndex()-index==1){
                    logEntryTreeMap.put(logEntry.getIndex(),logEntry);
                }
                return logEntry;
            });
        }else{
            if (lastLogIndex<index)
                return Future.succeededFuture();
            return Future.succeededFuture(logEntryTreeMap.get(index));
        }
    }

    public Future<LogEntry> getLastLog(){
        if (lastLogIndex==0)
            return Future.succeededFuture();

        return getLog(lastLogIndex);

    }



    public LogEntry addLog(String nodeId,int currentTerm, int requestId, Buffer buffer){
        ++lastLogIndex;
        LogEntry logEntry = new LogEntry().setIndex(lastLogIndex).setTerm(currentTerm).setNodeId(nodeId).setRequestId(requestId).setPayload(buffer);
        logEntryTreeMap.put(logEntry.getIndex(),logEntry);

        clusterDataStore.saveIndex(firstLogIndex,lastLogIndex);
        clusterDataStore.saveLog(logEntry);
        return logEntry;
    }

    public void setLog(LogEntry logEntry,boolean save){
        if (logEntryTreeMap.isEmpty()&&logEntry.getIndex()>lastLogIndex){
            logEntryTreeMap.put(logEntry.getIndex(),logEntry);
        } else if (logEntry.getIndex()>=listFirstIndex()){
            logEntryTreeMap.put(logEntry.getIndex(),logEntry);
        }
        //db
        if (logEntry.getIndex()>lastLogIndex) {
            lastLogIndex = logEntry.getIndex();
            if (save)
                clusterDataStore.saveIndex(firstLogIndex,lastLogIndex);
        }
        if (save)
            clusterDataStore.saveLog(logEntry);
    }


    /**
     * 删除index前的日志
     * @param index inclusive
     */
    public void truncatePrefix(int index){

        releasePrefix(index);

        clusterDataStore.delLog(0,index+1);
        firstLogIndex=index+1;
        clusterDataStore.saveIndex(firstLogIndex,lastLogIndex);
    }

    /**
     * 删除start后所有日志
     * @param start inclusive
     */
    public void truncateSuffix(int start){
        Integer key=null;
        while ((key=logEntryTreeMap.higherKey(start))!=null){
            logEntryTreeMap.remove(key);
        }

        clusterDataStore.delLog(start,lastLogIndex+1);
        lastLogIndex=start-1;
        clusterDataStore.saveIndex(firstLogIndex,lastLogIndex);
    }


    /**
     *
     * @param end inclusive
     */
    public void releasePrefix(int end){

        Integer key=null;
        while ((key=logEntryTreeMap.floorKey(end))!=null){
            logEntryTreeMap.remove(key);
        }
    }

    private int listFirstIndex(){
        Map.Entry<Integer, LogEntry> firstEntry = logEntryTreeMap.firstEntry();
        return firstEntry==null?lastLogIndex+1:firstEntry.getKey();
    }

    public int getFirstLogIndex() {
        return firstLogIndex;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

}
