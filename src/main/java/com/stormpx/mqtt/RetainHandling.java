package com.stormpx.mqtt;


public enum RetainHandling {

    SEND_MESSAGES_AT_THE_TIME(0),
    NOT_EXIST_SEND_MESSAGES_AT_THE_TIME(1),
    DO_NOT_SEND(2);


    private int value;

    RetainHandling(int value) {
        this.value=value;
    }

    public int getValue() {
        return value;
    }

    public static RetainHandling valueOf(int value){
        for (RetainHandling retainHandling : values()) {
            if (retainHandling.value==value)
                return retainHandling;
        }
        throw new IllegalArgumentException(value+" invalid retainHandling");
    }
}
