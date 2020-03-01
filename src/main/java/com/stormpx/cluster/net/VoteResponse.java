package com.stormpx.cluster.net;

public class VoteResponse {
    private String nodeId;
    private int term;
    private boolean preVote;
    private boolean voteGranted;

    public int getTerm() {
        return term;
    }

    public VoteResponse setTerm(int term) {
        this.term = term;
        return this;
    }

    public boolean isVoteGranted() {
        return voteGranted;
    }

    public VoteResponse setVoteGranted(boolean voteGranted) {
        this.voteGranted = voteGranted;
        return this;
    }

    public String getNodeId() {
        return nodeId;
    }

    public VoteResponse setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public boolean isPreVote() {
        return preVote;
    }

    public VoteResponse setPreVote(boolean preVote) {
        this.preVote = preVote;
        return this;
    }
}
