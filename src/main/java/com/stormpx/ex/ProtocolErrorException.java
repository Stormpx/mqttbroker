package com.stormpx.ex;

public class ProtocolErrorException extends RuntimeException {
    public ProtocolErrorException() {
    }

    public ProtocolErrorException(String message) {
        super(message);
    }

    public ProtocolErrorException(String message, Throwable cause) {
        super(message, cause);
    }

    public ProtocolErrorException(Throwable cause) {
        super(cause);
    }

    public ProtocolErrorException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
