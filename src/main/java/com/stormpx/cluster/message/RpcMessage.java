package com.stormpx.cluster.message;

import io.vertx.core.buffer.Buffer;

public class RpcMessage {
    private MessageType messageType;
    private String targetId;
    private String fromId;
    private int requestId;
    private Buffer buffer;

    public RpcMessage(MessageType messageType, String targetId, String fromId, int requestId, Buffer buffer) {
        this.messageType = messageType;
        this.targetId = targetId;
        this.fromId = fromId;
        this.requestId = requestId;
        this.buffer = buffer;
    }

   /* public static Buffer encode(MessageType messageType, String nodeId, int requestId, Buffer payload){
        Buffer id = Buffer.buffer(nodeId, "utf-8");
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
*/
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


    public String getTargetId() {
        return targetId;
    }

    public RpcMessage setTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public String getFromId() {
        return fromId;
    }

    public RpcMessage setFromId(String fromId) {
        this.fromId = fromId;
        return this;
    }

    public Buffer encode(){
        Buffer targetId = Buffer.buffer(this.targetId, "utf-8");
        Buffer fromId = Buffer.buffer(this.fromId, "utf-8");
        int size = 1 +  2 + targetId.length() +2 + fromId.length() + 4 + buffer.length();
        if (messageType==MessageType.REQUEST||messageType==MessageType.RESPONSE)
            size+=4;
        Buffer buffer = Buffer.buffer(size);
        buffer.appendByte((byte) messageType.getValue());

        buffer.appendUnsignedShort(targetId.length())
                .appendBuffer(targetId);

        buffer.appendUnsignedShort(fromId.length())
                .appendBuffer(fromId);
        if (messageType==MessageType.REQUEST||messageType==MessageType.RESPONSE)
            buffer.appendInt(requestId);

        buffer.appendInt(buffer.length())
                .appendBuffer(buffer);

        return buffer;
    }
}
