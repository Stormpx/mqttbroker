package com.stormpx.cluster.net;

public class AppendEntriesResponse {
    private int requestId;
    private String nodeId;
    private int term;
    private int requestLastIndex;
    private boolean success;

    public int getRequestId() {
        return requestId;
    }

    public AppendEntriesResponse setRequestId(int requestId) {
        this.requestId = requestId;
        return this;
    }

    public int getTerm() {
        return term;
    }

    public AppendEntriesResponse setTerm(int term) {
        this.term = term;
        return this;
    }

    public boolean isSuccess() {
        return success;
    }

    public AppendEntriesResponse setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public String getNodeId() {
        return nodeId;
    }

    public AppendEntriesResponse setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public int getRequestLastIndex() {
        return requestLastIndex;
    }

    public AppendEntriesResponse setRequestLastIndex(int requestLastIndex) {
        this.requestLastIndex = requestLastIndex;
        return this;
    }
}
