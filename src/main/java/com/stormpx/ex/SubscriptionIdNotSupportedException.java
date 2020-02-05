package com.stormpx.ex;

public class SubscriptionIdNotSupportedException extends RuntimeException {
    public SubscriptionIdNotSupportedException() {
    }

    public SubscriptionIdNotSupportedException(String message) {
        super(message);
    }

    public SubscriptionIdNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public SubscriptionIdNotSupportedException(Throwable cause) {
        super(cause);
    }

    public SubscriptionIdNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
