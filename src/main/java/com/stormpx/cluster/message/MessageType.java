package com.stormpx.cluster.message;

public enum  MessageType {

    APPENDENTRIESREQUEST(1),
    VOTEREQUEST(2),
    REQUEST(3),
    READINDEXREQUEST(4),
    APPENDENTRIESRESPONSE(5),
    VOTERESPONSE(6),
    RESPONSE(7),
    READINDEXRESPONSE(8)
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
