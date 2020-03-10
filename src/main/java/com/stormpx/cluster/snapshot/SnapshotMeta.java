package com.stormpx.cluster.snapshot;

public class SnapshotMeta {
    private String nodeId;
    private int index;
    private int term;

    public SnapshotMeta(String nodeId, int index, int term) {
        this.nodeId = nodeId;
        this.index = index;
        this.term = term;
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }
}
