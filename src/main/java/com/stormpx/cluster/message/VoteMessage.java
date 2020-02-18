package com.stormpx.cluster.message;


public class VoteMessage {
    private boolean preVote;
    private int term;
    private String candidateId;
    private int lastLogIndex;
    private int lastLogTerm;

    public int getTerm() {
        return term;
    }

    public VoteMessage setTerm(int term) {
        this.term = term;
        return this;
    }

    public String getCandidateId() {
        return candidateId;
    }

    public VoteMessage setCandidateId(String candidateId) {
        this.candidateId = candidateId;
        return this;
    }

    public int getLastLogIndex() {
        return lastLogIndex;
    }

    public VoteMessage setLastLogIndex(int lastLogIndex) {
        this.lastLogIndex = lastLogIndex;
        return this;
    }

    public int getLastLogTerm() {
        return lastLogTerm;
    }

    public VoteMessage setLastLogTerm(int lastLogTerm) {
        this.lastLogTerm = lastLogTerm;
        return this;
    }

    public boolean isPreVote() {
        return preVote;
    }

    public VoteMessage setPreVote(boolean preVote) {
        this.preVote = preVote;
        return this;
    }
}
