package com.stormpx.cluster;


import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.concurrent.ThreadLocalRandom;

public class ClusterState {
    private final static Logger logger= LoggerFactory.getLogger(ClusterState.class);
    private String id;
    private int currentTerm;
    private String votedFor;
    private int termFirstIndex;
    private int lastIndex;
    private int commitIndex;
    private int lastApplied;
    private int compactInterval;

    private LogList logList;


    public ClusterState() {
        ThreadLocalRandom localRandom = ThreadLocalRandom.current();
        this.compactInterval = localRandom.nextInt(8500, 12500);
    }



    public ClusterState setId(String id) {
        this.id = id;
        return this;
    }





    public ClusterState markTermFirstIndex() {
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

    public int getCompactInterval() {
        return compactInterval;
    }
}
