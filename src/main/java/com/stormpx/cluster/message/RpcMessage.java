package com.stormpx.cluster.message;

import io.vertx.core.buffer.Buffer;

public class RpcMessage {
    private MessageType messageType;
    private int requestId;
    private Buffer buffer;

    public MessageType getMessageType() {
        return messageType;
    }

    public RpcMessage setMessageType(MessageType messageType) {
        this.messageType = messageType;
        return this;
    }

    public int getRequestId() {
        return requestId;
    }

    public RpcMessage setRequestId(int requestId) {
        this.requestId = requestId;
        return this;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public RpcMessage setBuffer(Buffer buffer) {
        this.buffer = buffer;
        return this;
    }
}
