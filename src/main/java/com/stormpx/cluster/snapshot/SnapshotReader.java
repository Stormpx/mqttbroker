package com.stormpx.cluster.snapshot;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;


public class SnapshotReader {
    private final static Logger logger= LoggerFactory.getLogger(SnapshotReader.class);
    private Snapshot snapshot;
    private String nodeId;
    private AsyncFile asyncFile;

    private boolean end;
    private int offset;
    private SnapshotChunk snapshotChunk;

    public SnapshotReader(Snapshot snapshot,String nodeId,AsyncFile asyncFile) {
        this.snapshot=snapshot;
        this.nodeId=nodeId;
        this.asyncFile = asyncFile;
    }

    public Future<Buffer> readAll(){
        Promise<Buffer> promise=Promise.promise();
        Buffer buffer=Buffer.buffer(2048);
        nextChunk()
                .onFailure(promise::tryFail)
                .onSuccess(b->{
                    buffer.appendBuffer(b);
                    if (!end){
                        offset+=b.length();
                        readAll().map(buffer::appendBuffer).setHandler(promise);
                    }else{
                        promise.tryComplete(buffer);
                    }
                });
        return promise.future();
    }


    public Future<Buffer> nextChunk(){
        Promise<Buffer> promise=Promise.promise();
        asyncFile.read(Buffer.buffer(),0,offset,2048,ar->{
            if (ar.succeeded()){
                this.snapshotChunk=new SnapshotChunk(offset,ar.result());
                if (this.snapshotChunk.buffer.length()==0||this.snapshotChunk.buffer.length()<2048){
                    end=true;
                }
                promise.tryComplete(snapshotChunk.buffer);
            }else{
                promise.tryFail(ar.cause());
            }
        });

        return promise.future();
    }

    public void done(){
        snapshot.readDone(nodeId);

    }


    public int getOffset(){
        return offset;
    }

    public SnapshotChunk getCurrentChunk() {
        return snapshotChunk;
    }

    public boolean isEnd() {
        return end;
    }

    public SnapshotReader setOffset(int offset) {
        this.offset = offset;
        return this;
    }


    public class SnapshotChunk {
        private int offset;
        private Buffer buffer;

        public SnapshotChunk(int offset, Buffer buffer) {
            this.offset = offset;
            this.buffer = buffer;
        }

        public int getOffset() {
            return offset;
        }

        public Buffer getBuffer() {
            return buffer;
        }
    }
}
