package com.stormpx.cluster.message;

import io.vertx.core.buffer.Buffer;

public class ClusterMessage {
    private MessageType messageType;
    private String targetId;
    private String fromId;
    private Buffer buffer;

    public ClusterMessage(MessageType messageType, String targetId, String fromId,  Buffer buffer) {
        this.messageType = messageType;
        this.targetId = targetId;
        this.fromId = fromId;
        this.buffer = buffer;
    }

    public MessageType getMessageType() {
        return messageType;
    }

    public ClusterMessage setMessageType(MessageType messageType) {
        this.messageType = messageType;
        return this;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public ClusterMessage setBuffer(Buffer buffer) {
        this.buffer = buffer;
        return this;
    }


    public String getTargetId() {
        return targetId;
    }

    public ClusterMessage setTargetId(String targetId) {
        this.targetId = targetId;
        return this;
    }

    public String getFromId() {
        return fromId;
    }

    public ClusterMessage setFromId(String fromId) {
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

        buffer.appendInt(this.buffer.length())
                .appendBuffer(this.buffer);

        return buffer;
    }
}
