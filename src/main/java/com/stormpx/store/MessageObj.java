package com.stormpx.store;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;
import io.vertx.core.json.JsonObject;

import java.time.Instant;

public class MessageObj {
    public final static MessageObjCodec CODEC=new MessageObjCodec();

    private int refCnt =0;

    private long timestamp= Instant.now().getEpochSecond();

    private JsonObject message;

    public MessageObj(JsonObject message) {
        this.message = message;
    }

    public JsonObject getMessage() {
        return message;
    }



    public int add(int rnf){
        this.refCnt +=rnf;
        return this.refCnt;
    }

    public int getRefCnt() {
        return refCnt;
    }

    public long getTimestamp() {
        return timestamp;
    }

    @Override
    public String toString() {
        return "MessageObj{" + "refCnt=" + refCnt + ", message=" + message + '}';
    }



    private static class MessageObjCodec implements MessageCodec<MessageObj,MessageObj> {


        @Override
        public void encodeToWire(Buffer buffer, MessageObj topicMatchResult) {

        }

        @Override
        public MessageObj decodeFromWire(int pos, Buffer buffer) {
            return null;
        }

        @Override
        public MessageObj transform(MessageObj messageObj) {
            return messageObj;
        }

        @Override
        public String name() {
            return "messageObj";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
