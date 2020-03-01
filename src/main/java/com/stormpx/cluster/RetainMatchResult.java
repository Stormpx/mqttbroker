package com.stormpx.cluster;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.Map;
import java.util.Set;

public class RetainMatchResult {
    public final static RetainMatchReslutCodec CODEC=new RetainMatchReslutCodec();

    private Map<String, Set<String>> matchMap;

    public Map<String, Set<String>> getMatchMap() {
        return matchMap;
    }

    public RetainMatchResult setMatchMap(Map<String, Set<String>> matchMap) {
        this.matchMap = matchMap;
        return this;
    }

    private static class RetainMatchReslutCodec implements MessageCodec<RetainMatchResult, RetainMatchResult> {


        @Override
        public void encodeToWire(Buffer buffer, RetainMatchResult retainMatchResult) {

        }

        @Override
        public RetainMatchResult decodeFromWire(int pos, Buffer buffer) {
            return null;
        }

        @Override
        public RetainMatchResult transform(RetainMatchResult retainMatchResult) {
            return retainMatchResult;
        }

        @Override
        public String name() {
            return "RetainMatchResult";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
