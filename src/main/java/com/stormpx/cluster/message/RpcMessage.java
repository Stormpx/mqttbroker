package com.stormpx.cluster.message;

import io.vertx.core.buffer.Buffer;

public class RpcMessage {
    private MessageType messageType;
    private String fromId;
    private int requestId;
    private Buffer buffer;

    public static Buffer encode(MessageType messageType,String fromId,int requestId,Buffer payload){
        Buffer id = Buffer.buffer(fromId, "utf-8");
        int size = 1 + 2 + id.length() + 4 + payload.length();
        if (messageType==MessageType.REQUEST||messageType==MessageType.RESPONSE)
            size+=4;
        Buffer buffer = Buffer.buffer(size);
        buffer.appendByte((byte) messageType.getValue());
        buffer.appendUnsignedShort(id.length())
                .appendBuffer(id);
        if (messageType==MessageType.REQUEST||messageType==MessageType.RESPONSE)
            buffer.appendInt(requestId);

        buffer.appendInt(payload.length())
            .appendBuffer(payload);

        return buffer;
    }

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

    public String getFromId() {
        return fromId;
    }

    public RpcMessage setFromId(String fromId) {
        this.fromId = fromId;
        return this;
    }
}
