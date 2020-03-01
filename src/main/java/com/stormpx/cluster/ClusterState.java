package com.stormpx.cluster;


import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class ClusterState {
    private final static Logger logger= LoggerFactory.getLogger(ClusterState.class);
    private String id;
    private int currentTerm;
    private String votedFor;
    private int termFirstIndex;
    private int lastIndex;
    private int commitIndex;
    private int lastApplied;

    private LogList logList;
    private Map<Integer, LogEntry> logMap;

    public ClusterState() {
        this.logMap =new HashMap<>();
    }



    public ClusterState setId(String id) {
        this.id = id;
        return this;
    }


    public LogEntry getLog(int index){
        return logMap.get(index);
    }

    public LogEntry addLog(String nodeId,int requestId,Buffer buffer){
        int index = ++lastIndex;
        LogEntry logEntry = new LogEntry().setIndex(index).setTerm(currentTerm).setNodeId(nodeId).setRequestId(requestId).setPayload(buffer);
        if (logger.isDebugEnabled()) {
            logger.error("add new log index:{} term:{} nodeId:{} requestId:{} buffer:{}", index, currentTerm, nodeId, requestId, buffer);
        }
        logMap.put(index, logEntry);
        return logEntry;
    }

    public void setLog(LogEntry logEntry){
        logMap.put(logEntry.getIndex(), logEntry);
    }

    public void delLog(int index){
        logMap.remove(index);
    }

    public ClusterState markTermFirstIndex() {
//        this.termFirstIndex = lastIndex;
        this.termFirstIndex = logList.getLastLogIndex();
        return this;
    }

    public int readIndex(){
        return Math.max(termFirstIndex,commitIndex);
    }


    public String getId() {
        return id;
    }
    public String getVotedFor() {
        return votedFor;
    }
    public int getCommitIndex() {
        return commitIndex;
    }
    public int getLastApplied() {
        return lastApplied;
    }
    public int getLastIndex() {
        return logList.getLastLogIndex();
//        return lastIndex;
    }

    public int getCurrentTerm() {
        return currentTerm;
    }


    public ClusterState setVotedFor(String votedFor) {
        this.votedFor = votedFor;
        return this;
    }


    public ClusterState setCommitIndex(int commitIndex) {
        this.commitIndex = commitIndex;
        return this;
    }


    public ClusterState setLastApplied(int lastApplied) {
        this.lastApplied = lastApplied;
        return this;
    }


    public ClusterState setCurrentTerm(int currentTerm) {
        this.currentTerm = currentTerm;
        return this;
    }


    public ClusterState setLastIndex(int lastIndex) {
        this.lastIndex = lastIndex;
        return this;
    }


    public ClusterState setLogList(LogList logList) {
        this.logList = logList;
        return this;
    }
}
