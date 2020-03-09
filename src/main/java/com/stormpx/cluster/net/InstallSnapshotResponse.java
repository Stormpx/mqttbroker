package com.stormpx.cluster.net;

public class InstallSnapshotResponse {
    private boolean accept;
    private int term;
    private int lastNum;


    public int getTerm() {
        return term;
    }

    public InstallSnapshotResponse setTerm(int term) {
        this.term = term;
        return this;
    }

    public int getLastNum() {
        return lastNum;
    }

    public InstallSnapshotResponse setLastNum(int lastNum) {
        this.lastNum = lastNum;
        return this;
    }

    public boolean isAccept() {
        return accept;
    }

    public InstallSnapshotResponse setAccept(boolean accept) {
        this.accept = accept;
        return this;
    }
}
