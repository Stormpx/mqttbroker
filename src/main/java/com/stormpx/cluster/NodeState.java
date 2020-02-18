package com.stormpx.cluster;

public class NodeState {

    private int nextIndex;
    private int matchIndex;

    public int getNextIndex() {
        return nextIndex;
    }

    public NodeState setNextIndex(int nextIndex) {
        this.nextIndex = nextIndex;
        return this;
    }

    public int getMatchIndex() {
        return matchIndex;
    }

    public NodeState setMatchIndex(int matchIndex) {
        this.matchIndex = matchIndex;
        return this;
    }
}
