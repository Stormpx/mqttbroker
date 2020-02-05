package com.stormpx.ex;

public class QosNotSupportedException extends RuntimeException {

    public QosNotSupportedException() {
    }

    public QosNotSupportedException(String message) {
        super(message);
    }

    public QosNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public QosNotSupportedException(Throwable cause) {
        super(cause);
    }

    public QosNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
