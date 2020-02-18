package com.stormpx.cluster.message;


import com.stormpx.cluster.LogEntry;

import java.util.List;

public class AppendEntriesMessage {
    private int term;
    private String leaderId;
    private int prevLogIndex;
    private int prevLogTerm;
    private List<LogEntry> entries;
    private int leaderCommit;


    public int getTerm() {
        return term;
    }

    public AppendEntriesMessage setTerm(int term) {
        this.term = term;
        return this;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public AppendEntriesMessage setLeaderId(String leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public AppendEntriesMessage setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public AppendEntriesMessage setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public AppendEntriesMessage setEntries(List<LogEntry> entries) {
        this.entries = entries;
        return this;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public AppendEntriesMessage setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
        return this;
    }
}
