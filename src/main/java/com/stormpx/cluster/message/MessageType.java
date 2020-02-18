package com.stormpx.cluster.message;

public enum  MessageType {

    APPENDENTRIES(1),
    VOTE(2),
    REQUEST(3)
    ;


    private int value;

    MessageType(int value) {
        this.value=value;
    }

    public int getValue() {
        return value;
    }

    public static MessageType valueOf(byte hex){
        for (MessageType messageType : values()) {
            if (messageType.value==hex){
                return messageType;
            }
        }
        return null;
    }

}
