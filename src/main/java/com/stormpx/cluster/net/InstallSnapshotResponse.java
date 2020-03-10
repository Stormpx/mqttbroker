package com.stormpx.cluster.net;

public class InstallSnapshotResponse {
    private boolean accept;
    private boolean done;
    private int term;
    private int nextOffset;


    public int getTerm() {
        return term;
    }

    public InstallSnapshotResponse setTerm(int term) {
        this.term = term;
        return this;
    }


    public boolean isAccept() {
        return accept;
    }

    public InstallSnapshotResponse setAccept(boolean accept) {
        this.accept = accept;
        return this;
    }

    public boolean isDone() {
        return done;
    }

    public InstallSnapshotResponse setDone(boolean done) {
        this.done = done;
        return this;
    }

    public int getNextOffset() {
        return nextOffset;
    }

    public InstallSnapshotResponse setNextOffset(int nextOffset) {
        this.nextOffset = nextOffset;
        return this;
    }
}
