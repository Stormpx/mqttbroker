package com.stormpx.cluster.mqtt;

import com.stormpx.store.SessionObj;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

public class SessionResult {
    public final static SessionResultCodec CODEC=new SessionResultCodec();
    private boolean isLocal;
    private SessionObj sessionObj;

    public boolean isLocal() {
        return isLocal;
    }

    public SessionResult setLocal(boolean local) {
        isLocal = local;
        return this;
    }

    public SessionObj getSessionObj() {
        return sessionObj;
    }

    public SessionResult setSessionObj(SessionObj sessionObj) {
        this.sessionObj = sessionObj;
        return this;
    }

    private static class SessionResultCodec implements MessageCodec<SessionResult,SessionResult> {


        @Override
        public void encodeToWire(Buffer buffer, SessionResult sessionResult) {

        }

        @Override
        public SessionResult decodeFromWire(int pos, Buffer buffer) {
            return null;
        }

        @Override
        public SessionResult transform(SessionResult sessionResult) {
            return sessionResult;
        }

        @Override
        public String name() {
            return "SessionResult";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
