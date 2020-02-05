package com.stormpx.ex;

public class WildcardSubscriptionsNotSupportedException extends RuntimeException {
    public WildcardSubscriptionsNotSupportedException() {
    }

    public WildcardSubscriptionsNotSupportedException(String message) {
        super(message);
    }

    public WildcardSubscriptionsNotSupportedException(String message, Throwable cause) {
        super(message, cause);
    }

    public WildcardSubscriptionsNotSupportedException(Throwable cause) {
        super(cause);
    }

    public WildcardSubscriptionsNotSupportedException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
