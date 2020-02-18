package com.stormpx.cluster;

public enum  MemberType {
    FOLLOWER(0),
    PRE_CANDIDATES(1),
    CANDIDATES(2),
    LEADER(3);


    private int value;
    MemberType(int i) {
        value=i;
    }
}
