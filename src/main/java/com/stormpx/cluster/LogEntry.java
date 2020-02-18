package com.stormpx.cluster;

import com.stormpx.kit.UnSafeJsonObject;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

public class LogEntry {
    public final static LogEntryCodec CODEC=new LogEntryCodec();

    private int term;
    private int index;
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

    private static class LogEntryCodec implements MessageCodec<LogEntry,LogEntry> {


        @Override
        public void encodeToWire(Buffer buffer, LogEntry logEntry) {
            buffer.appendInt(logEntry.index);
            buffer.appendInt(logEntry.term);
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
            pos+=4;
            int payloadLength = buffer.getInt(pos);
            if (payloadLength!=0) {
                pos+=4;
                logEntry.setPayload(buffer.getBuffer(pos,pos+payloadLength));
            }
            return logEntry.setIndex(index).setTerm(term);
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
