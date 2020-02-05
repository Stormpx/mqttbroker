package com.stormpx.ex;

public class RetainNotSupportedException extends RuntimeException {
    public RetainNotSupportedException() {
    }

    public RetainNotSupportedException(String message) {
        super(message);
    }

    public RetainNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public RetainNotSupportedException(Throwable cause) {
        super(cause);
    }

    public RetainNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
