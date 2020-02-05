package com.stormpx.ex;

public class InvalidTopicAliasException extends RuntimeException {

    public InvalidTopicAliasException() {
        super();
    }

    public InvalidTopicAliasException(String message) {
        super(message);
    }

    public InvalidTopicAliasException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidTopicAliasException(Throwable cause) {
        super(cause);
    }

    protected InvalidTopicAliasException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
