package com.stormpx.cluster;

import com.stormpx.kit.UnSafeJsonObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class LogEntry {
    public final static LogEntryCodec CODEC=new LogEntryCodec();

    private int term;
    private int index;
    private String nodeId;
    private int requestId;
    private Buffer payload;

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

    private static class LogEntryCodec implements MessageCodec<LogEntry,LogEntry> {


        @Override
        public void encodeToWire(Buffer buffer, LogEntry logEntry) {
            buffer.appendInt(logEntry.index);
            buffer.appendInt(logEntry.term);
            Buffer id = Buffer.buffer(logEntry.nodeId, "utf-8");
            buffer.appendUnsignedShort(id.length());
            buffer.appendBuffer(id);
            buffer.appendInt(logEntry.requestId);
            if (logEntry.payload==null){
                buffer.appendInt(0);
            }else {
                buffer.appendInt(logEntry.payload.length());
                buffer.appendBuffer(logEntry.payload);
            }
        }

        @Override
        public LogEntry decodeFromWire(int pos, Buffer buffer) {
            LogEntry logEntry = new LogEntry();
            int index = buffer.getInt(pos);
            pos+=4;
            int term = buffer.getInt(pos);
            pos+=2;
            int unsignedShort = buffer.getUnsignedShort(pos);
            pos+=2;
            String nodeId = buffer.getString(pos, pos + unsignedShort, "utf-8");
            pos+=unsignedShort;
            int requestId = buffer.getInt(pos);
            int payloadLength = buffer.getInt(pos);
            if (payloadLength!=0) {
                pos+=4;
                logEntry.setPayload(buffer.getBuffer(pos,pos+payloadLength));
            }
            return logEntry.setIndex(index).setTerm(term).setNodeId(nodeId).setRequestId(requestId);
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
