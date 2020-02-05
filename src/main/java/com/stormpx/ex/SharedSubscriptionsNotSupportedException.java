package com.stormpx.ex;

public class SharedSubscriptionsNotSupportedException extends RuntimeException {
    public SharedSubscriptionsNotSupportedException() {
    }

    public SharedSubscriptionsNotSupportedException(String message) {
        super(message);
    }

    public SharedSubscriptionsNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public SharedSubscriptionsNotSupportedException(Throwable cause) {
        super(cause);
    }

    public SharedSubscriptionsNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
