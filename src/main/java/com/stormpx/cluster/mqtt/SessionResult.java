package com.stormpx.cluster.mqtt;

import com.stormpx.dispatcher.ClientSession;
import com.stormpx.store.SessionObj;

public class SessionResult {
    private boolean isLocal;
    private ClientSession session;

    public boolean isLocal() {
        return isLocal;
    }

    public SessionResult setLocal(boolean local) {
        isLocal = local;
        return this;
    }

    public ClientSession getSession() {
        return session;
    }

    public SessionResult setSession(ClientSession session) {
        this.session = session;
        return this;
    }
}
