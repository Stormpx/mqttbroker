package com.stormpx.cluster.snapshot;

import io.vertx.core.buffer.Buffer;

public class SnapshotMeta {
    private String nodeId;
    private int index;
    private int term;

    public SnapshotMeta(String nodeId, int index, int term) {
        this.nodeId = nodeId;
        this.index = index;
        this.term = term;
    }

    public static SnapshotMeta decode(Buffer buffer){
        int pos=0;
        int nodeIdLength = buffer.getInt(pos);
        pos+=4;
        String nodeId = buffer.getString(pos, pos + nodeIdLength);
        pos+=nodeIdLength;
        int index = buffer.getInt(pos);
        pos+=4;
        int term = buffer.getInt(pos);
        return new SnapshotMeta(nodeId,index,term);
    }

    public String getNodeId() {
        return nodeId;
    }

    public int getIndex() {
        return index;
    }

    public int getTerm() {
        return term;
    }

    public Buffer encode(){
        byte[] nodeIdBytes = nodeId.getBytes();
        Buffer buffer = Buffer.buffer(4 + nodeIdBytes.length + 4 + 4);
        buffer.appendInt(nodeIdBytes.length)
                .appendBytes(nodeIdBytes);

        buffer.appendInt(index);
        buffer.appendInt(term);
        return buffer;
    }
}
