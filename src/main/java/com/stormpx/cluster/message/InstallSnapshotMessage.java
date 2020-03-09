package com.stormpx.cluster.message;

import io.vertx.core.buffer.Buffer;

public class InstallSnapshotMessage {
    private int term;
    private String leaderId;
    private int lastIncludeIndex;
    private int lastIncludeTerm;
    private boolean done;
    private int num;
    private Buffer buffer;

    public static InstallSnapshotMessage decode(Buffer buffer){
        int pos=0;
        int term = buffer.getInt(pos);
        pos+=4;
        int leaderIdLength = buffer.getInt(pos);
        pos+=4;
        String leaderId = buffer.getString(pos, pos + leaderIdLength);
        pos+=leaderIdLength;
        int lastIncludeIndex = buffer.getInt(pos);
        pos+=4;
        int lastIncludeTerm = buffer.getInt(pos);
        pos+=4;
        boolean done = buffer.getByte(pos) == 1;
        pos+=1;
        int num = buffer.getInt(pos);
        Buffer slice = buffer.slice(pos, buffer.length());
        InstallSnapshotMessage installSnapshotMessage = new InstallSnapshotMessage();
        installSnapshotMessage.setTerm(term).setLeaderId(leaderId).setLastIncludeIndex(lastIncludeIndex).setLastIncludeTerm(lastIncludeTerm)
                .setDone(done).setNum(num).setBuffer(slice);
        return installSnapshotMessage;
    }

    public Buffer encode(){
        byte[] bytes = leaderId.getBytes();
        int size=4+4+bytes.length+4+4+1+4+buffer.length();
        Buffer buffer = Buffer.buffer(size);

        buffer.appendInt(term);
        buffer.appendInt(bytes.length)
                .appendBytes(bytes);
        buffer.appendInt(lastIncludeIndex)
                .appendInt(lastIncludeTerm);

        buffer.appendByte((byte) (done?1:0));
        buffer.appendInt(num);
        buffer.appendBuffer(buffer);
        return buffer;
    }

    public int getTerm() {
        return term;
    }

    public InstallSnapshotMessage setTerm(int term) {
        this.term = term;
        return this;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public InstallSnapshotMessage setLeaderId(String leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public int getLastIncludeIndex() {
        return lastIncludeIndex;
    }

    public InstallSnapshotMessage setLastIncludeIndex(int lastIncludeIndex) {
        this.lastIncludeIndex = lastIncludeIndex;
        return this;
    }

    public int getLastIncludeTerm() {
        return lastIncludeTerm;
    }

    public InstallSnapshotMessage setLastIncludeTerm(int lastIncludeTerm) {
        this.lastIncludeTerm = lastIncludeTerm;
        return this;
    }

    public boolean isDone() {
        return done;
    }

    public InstallSnapshotMessage setDone(boolean done) {
        this.done = done;
        return this;
    }

    public int getNum() {
        return num;
    }

    public InstallSnapshotMessage setNum(int num) {
        this.num = num;
        return this;
    }

    public Buffer getBuffer() {
        return buffer;
    }

    public InstallSnapshotMessage setBuffer(Buffer buffer) {
        this.buffer = buffer;
        return this;
    }
}
