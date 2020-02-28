package com.stormpx.cluster.message;


import com.stormpx.cluster.LogEntry;
import io.netty.buffer.ByteBuf;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class AppendEntriesMessage {
    private int term;
    private int prevLogIndex;
    private int prevLogTerm;
    private int leaderCommit;
    private String leaderId;
    private List<LogEntry> entries;

    public static AppendEntriesMessage decode(Buffer buffer){
        int pos=0;

        int term = buffer.getInt(pos);
        pos+=4;
        int prevLogIndex = buffer.getInt(pos);
        pos+=4;
        int prevLogTerm = buffer.getInt(pos);
        pos+=4;
        int leaderCommit = buffer.getInt(pos);
        pos+=4;
        int leaderIdBytesLength = buffer.getUnsignedShort(pos);
        pos+=2;
        String leaderId = buffer.slice(pos,pos+leaderIdBytesLength).toString(StandardCharsets.UTF_8);
        pos+=leaderIdBytesLength;

        List<LogEntry> logEntries=new ArrayList<>();
        while (pos<buffer.length()){
            LogEntry logEntry = LogEntry.decode(pos, buffer);
            logEntries.add(logEntry);
            pos+=logEntry.length();
        }

        AppendEntriesMessage appendEntriesMessage = new AppendEntriesMessage();
        appendEntriesMessage.term=term;
        appendEntriesMessage.prevLogIndex=prevLogIndex;
        appendEntriesMessage.prevLogTerm=prevLogTerm;
        appendEntriesMessage.leaderCommit=leaderCommit;
        appendEntriesMessage.leaderId=leaderId;
        appendEntriesMessage.entries=logEntries;
        return appendEntriesMessage;
    }

    public Buffer encode(){
        Integer entriesLength = entries.stream().map(LogEntry::length).reduce(0, (i1, i2) -> i1 + i2);
        byte[] leaderIdBytes = leaderId.getBytes(StandardCharsets.UTF_8);
        Buffer buffer = Buffer.buffer(4 + 4 + 4 + 4 + 2 + leaderIdBytes.length + 4 + entriesLength);

        buffer.appendInt(term);
        buffer.appendInt(prevLogIndex);
        buffer.appendInt(prevLogTerm);
        buffer.appendInt(leaderCommit);
        buffer.appendUnsignedShort(leaderIdBytes.length)
                .appendBytes(leaderIdBytes);

        entries.forEach(logEntry -> logEntry.encode(buffer));
        return buffer;
    }

    public int getTerm() {
        return term;
    }

    public AppendEntriesMessage setTerm(int term) {
        this.term = term;
        return this;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public AppendEntriesMessage setLeaderId(String leaderId) {
        this.leaderId = leaderId;
        return this;
    }

    public int getPrevLogIndex() {
        return prevLogIndex;
    }

    public AppendEntriesMessage setPrevLogIndex(int prevLogIndex) {
        this.prevLogIndex = prevLogIndex;
        return this;
    }

    public int getPrevLogTerm() {
        return prevLogTerm;
    }

    public AppendEntriesMessage setPrevLogTerm(int prevLogTerm) {
        this.prevLogTerm = prevLogTerm;
        return this;
    }

    public List<LogEntry> getEntries() {
        return entries;
    }

    public AppendEntriesMessage setEntries(List<LogEntry> entries) {
        this.entries = entries;
        return this;
    }

    public int getLeaderCommit() {
        return leaderCommit;
    }

    public AppendEntriesMessage setLeaderCommit(int leaderCommit) {
        this.leaderCommit = leaderCommit;
        return this;
    }


}
