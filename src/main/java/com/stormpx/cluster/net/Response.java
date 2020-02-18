package com.stormpx.cluster.net;

import io.vertx.core.buffer.Buffer;

public class Response {
    private boolean success;
    private int requestId;
    private Buffer payload;

    public boolean isSuccess() {
        return success;
    }

    public Response setSuccess(boolean success) {
        this.success = success;
        return this;
    }

    public int getRequestId() {
        return requestId;
    }

    public Response setRequestId(int requestId) {
        this.requestId = requestId;
        return this;
    }

    public Buffer getPayload() {
        return payload;
    }

    public Response setPayload(Buffer payload) {
        this.payload = payload;
        return this;
    }
}

