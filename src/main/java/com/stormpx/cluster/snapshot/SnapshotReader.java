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
    private Buffer currentChunk;

    public SnapshotReader(Snapshot snapshot,String nodeId,AsyncFile asyncFile) {
        this.snapshot=snapshot;
        this.nodeId=nodeId;
        this.asyncFile = asyncFile;
        this.currentChunk=Buffer.buffer();
    }

    public Future<Buffer> readAll(){
        Promise<Buffer> promise=Promise.promise();
        Buffer buffer=Buffer.buffer(2048);
        nextChunk()
                .onFailure(promise::tryFail)
                .onSuccess(b->{
                    buffer.appendBuffer(b);
                    if (!end){
                        readAll().onSuccess(buffer::appendBuffer).setHandler(promise);
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
                this.currentChunk= ar.result();
                if (this.currentChunk.length()==0){
                    end=true;
                }
                promise.tryComplete();
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

    public Buffer getCurrentChunk() {
        return currentChunk.copy();
    }

    public boolean isEnd() {
        return end;
    }

    public SnapshotReader setOffset(int offset) {
        this.offset = offset;
        return this;
    }
}
