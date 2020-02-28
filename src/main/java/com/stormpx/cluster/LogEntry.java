package com.stormpx.cluster;

import com.stormpx.kit.UnSafeJsonObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class LogEntry {
    public final static LogEntryCodec CODEC=new LogEntryCodec();

    private int term;
    private int index;
    private int requestId;
    private String nodeId;
    private Buffer payload;

    private Integer length=null;

    public int length(){
        if (length!=null)
            return length;
        length=4+4+4+2+nodeId.getBytes(StandardCharsets.UTF_8).length+4+(payload==null?0:payload.length());
        return length;
    }

    public LogEntry length(int length){
        this.length=length;
        return this;
    }


    public int getTerm() {
        return term;
    }

    public LogEntry setTerm(int term) {
        this.term = term;
        return this;
    }

    public int getIndex() {
        return index;
    }

    public LogEntry setIndex(int index) {
        this.index = index;
        return this;
    }

    public Buffer getPayload() {
        return payload;
    }

    public LogEntry setPayload(Buffer payload) {
        this.payload = payload;
        return this;
    }

    public String getNodeId() {
        return nodeId;
    }

    public LogEntry setNodeId(String nodeId) {
        this.nodeId = nodeId;
        return this;
    }

    public int getRequestId() {
        return requestId;
    }

    public LogEntry setRequestId(int requestId) {
        this.requestId = requestId;
        return this;
    }

    public static LogEntry decode(int pos,Buffer buffer){
        int org=pos;
        int index = buffer.getInt(pos);
        pos+=4;
        int term = buffer.getInt(pos);
        pos+=4;
        int requestId = buffer.getInt(pos);
        pos+=4;
        int nodeIdLength = buffer.getUnsignedShort(pos);
        pos+=2;
        String nodeId = buffer.getString(pos, pos + nodeIdLength, "utf-8");
        pos+=nodeIdLength;
        int payloadLength = buffer.getInt(pos);
        pos+=4;
        Buffer payload = buffer.getBuffer(pos, pos + payloadLength);
        pos+=payloadLength;

        LogEntry logEntry = new LogEntry().setIndex(index).setTerm(term).setRequestId(requestId).setNodeId(nodeId).setPayload(payload);

        logEntry.length=pos-org;
        return logEntry;
    }

    public Buffer encode(){
        Buffer buffer=Buffer.buffer(length());
        return encode(buffer);
    }

    public Buffer encode(Buffer buffer){
        byte[] nodeIdBytes = nodeId.getBytes(StandardCharsets.UTF_8);
        buffer.appendInt(index);
        buffer.appendInt(term);
        buffer.appendInt(requestId);

        buffer.appendUnsignedShort(nodeIdBytes.length)
                .appendBytes(nodeIdBytes);

        if (payload==null){
            buffer.appendInt(0);
        }else {
            buffer.appendInt(payload.length());
            buffer.appendBuffer(payload);
        }
        return buffer;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LogEntry logEntry = (LogEntry) o;
        return term == logEntry.term && index == logEntry.index && requestId == logEntry.requestId && Objects.equals(nodeId, logEntry.nodeId) && Objects.equals(payload, logEntry.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(term, index, requestId, nodeId, payload);
    }

    private static class LogEntryCodec implements MessageCodec<LogEntry,LogEntry> {


        @Override
        public void encodeToWire(Buffer buffer, LogEntry logEntry) {
            buffer.appendBuffer(logEntry.encode());
        }

        @Override
        public LogEntry decodeFromWire(int pos, Buffer buffer) {
            return decode(pos,buffer);
        }

        @Override
        public LogEntry transform(LogEntry logEntry) {
            return logEntry;
        }

        @Override
        public String name() {
            return "logEntry";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
