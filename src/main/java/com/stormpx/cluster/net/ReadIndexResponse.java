package com.stormpx.cluster.net;

public class ReadIndexResponse {
    private String id;
    private boolean isLeader;
    private int readIndex;

    public boolean isLeader() {
        return isLeader;
    }

    public ReadIndexResponse setLeader(boolean leader) {
        isLeader = leader;
        return this;
    }

    public int getReadIndex() {
        return readIndex;
    }

    public ReadIndexResponse setReadIndex(int readIndex) {
        this.readIndex = readIndex;
        return this;
    }

    public String getId() {
        return id;
    }

    public ReadIndexResponse setId(String id) {
        this.id = id;
        return this;
    }
}
