package com.stormpx.dispatcher.command;

import com.stormpx.dispatcher.DispatcherMessage;

public class CloseSessionCommand {
    private String clientId;
    private boolean disconnect;
    private boolean takenOver;
    private long sessionExpiryInterval;
    private boolean will;
    private long willDelayInterval;
    private DispatcherMessage willMessage;

    public CloseSessionCommand(String clientId) {
        this.clientId = clientId;
    }


    public CloseSessionCommand setSessionExpiryInterval(long sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
        return this;
    }

    public CloseSessionCommand setWill(boolean will) {
        this.will = will;
        return this;
    }

    public CloseSessionCommand setWillDelayInterval(long willDelayInterval) {
        this.willDelayInterval = willDelayInterval;
        return this;
    }

    public CloseSessionCommand setWillMessage(DispatcherMessage willMessage) {
        this.willMessage = willMessage;
        return this;
    }

    public String getClientId() {
        return clientId;
    }

    public boolean isDisconnect() {
        return disconnect;
    }

    public long getSessionExpiryInterval() {
        return sessionExpiryInterval;
    }

    public boolean isWill() {
        return will;
    }

    public long getWillDelayInterval() {
        return willDelayInterval;
    }

    public DispatcherMessage getWillMessage() {
        return willMessage;
    }

    public boolean isTakenOver() {
        return takenOver;
    }

    public CloseSessionCommand setTakenOver(boolean takenOver) {
        this.takenOver = takenOver;
        return this;
    }

    public CloseSessionCommand setDisconnect(boolean disconnect) {
        this.disconnect = disconnect;
        return this;
    }

    @Override
    public String toString() {
        return "CloseSessionCommand{" + "clientId='" + clientId + '\'' + ", disconnect=" + disconnect + ", takenOver=" + takenOver + ", sessionExpiryInterval=" + sessionExpiryInterval + ", will=" + will + ", willDelayInterval=" + willDelayInterval + ", willMessage=" + willMessage + '}';
    }
}
