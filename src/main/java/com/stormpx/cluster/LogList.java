package com.stormpx.cluster;

import com.stormpx.store.ClusterDataStore;
import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;

import java.util.*;

public class LogList {
    private ClusterDataStore clusterDataStore;
    private int firstLogIndex;
    private int lastLogIndex;

    private List<LogEntry> list;
    private int listFirstIndex;

    public LogList(ClusterDataStore clusterDataStore, int firstLogIndex, int lastLogIndex) {
        this.clusterDataStore = clusterDataStore;
        this.firstLogIndex = firstLogIndex;
        this.lastLogIndex = lastLogIndex;
        this.list=new LinkedList<>();
        this.listFirstIndex=lastLogIndex+1;
    }


    public Future<List<LogEntry>> getLog(int start, int end){
        if (start>end)
            return Future.succeededFuture(Collections.emptyList());
        if (start>=listFirstIndex){
            return Future.succeededFuture(list.subList(start-listFirstIndex,Math.min(end-listFirstIndex,list.size())));
        }else {
            return clusterDataStore.getLogs(start, end)
                    .map(list -> {
                        if (list.isEmpty()) return list;

                        if (end-1>=listFirstIndex){

                            this.list.addAll(0,list.subList(0,list.size()-(end-listFirstIndex)));
                            this.listFirstIndex=start<=0?1:start;
                        }

                        return list;
                    });
        }
    }

    public Future<LogEntry> getLog(int index){
        if (index<listFirstIndex){
            //db
            return clusterDataStore.getLogs(index,index+1).map(list->{
                if (list.isEmpty())
                    return null;
                LogEntry logEntry = list.get(0);
                if (lastLogIndex-index==1){
                    this.list.add(0,logEntry);
                    lastLogIndex=index;
                }
                return logEntry;
            });
        }else{
            if (lastLogIndex<index)
                return Future.succeededFuture();
            return Future.succeededFuture(list.get(index-listFirstIndex));
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
        list.add(logEntry);

        if (list.size()==1)
            listFirstIndex=logEntry.getIndex();

        clusterDataStore.saveLog(logEntry);
        return logEntry;
    }

    public void setLog(LogEntry logEntry){
        if (logEntry.getIndex()>=listFirstIndex){
            list.set(logEntry.getIndex()-listFirstIndex,logEntry);
        }
        //db
        clusterDataStore.saveLog(logEntry);
    }


    /**
     * 删除index前的日志
     * @param index inclusive
     */
    public void truncatePrefix(int index){
        if (listFirstIndex<=index){
            list.removeAll(list.subList(0,Math.min(index-listFirstIndex+1,list.size())));
        }
        clusterDataStore.delLog(0,index+1);
        firstLogIndex=index+1;
    }

    /**
     * 删除start后所有日志
     * @param start inclusive
     */
    public void truncateSuffix(int start){
        if (listFirstIndex<=start){
            int startIndex=Math.min(start-listFirstIndex,list.size());
            list.removeAll(list.subList(startIndex,list.size()));
        }
        clusterDataStore.delLog(start,lastLogIndex+1);
        lastLogIndex=start-1;
    }


    /**
     *
     * @param end inclusive
     */
    public void releasePrefix(int end){

        end=Math.min(end,lastLogIndex);
        if (listFirstIndex<=end){
            int toIndex=Math.min(list.size(),end-listFirstIndex);
            this.list=list.subList(toIndex+1,list.size());
            if (list.isEmpty())
                listFirstIndex=lastLogIndex+1;
            else
                listFirstIndex=end+1;
        }
    }


    public int getFirstLogIndex() {
        return firstLogIndex;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

}
