package com.stormpx.ex;

public class PacketTooLagerException extends RuntimeException{
    public PacketTooLagerException() {
    }

    public PacketTooLagerException(String message) {
        super(message);
    }

    public PacketTooLagerException(String message, Throwable cause) {
        super(message, cause);
    }

    public PacketTooLagerException(Throwable cause) {
        super(cause);
    }

    public PacketTooLagerException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
