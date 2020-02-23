package com.stormpx.cluster.message;

public enum RequestType {

    SESSION(1),
    MESSAGE(2),
    TAKENOVER(3),
    PUBLISH(4),
    ADDLOG(8);


    private int value;
    RequestType(int i) {
        this.value=i;
    }

    public int getValue() {
        return value;
    }

    public static RequestType valueOf(byte value){
        for (RequestType requestType : values()) {
            if (requestType.value==value)
                return requestType;
        }
        return null;
    }

}
