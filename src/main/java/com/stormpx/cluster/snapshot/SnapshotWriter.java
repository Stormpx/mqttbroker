package com.stormpx.cluster.snapshot;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class SnapshotWriter {
    private final static Logger logger= LoggerFactory.getLogger(SnapshotWriter.class);
    private SnapshotMeta snapshotMeta;
    private String path;
    private int offset;
    private AsyncFile asyncFile;
    private boolean end;
    private Promise<Promise<Void>> promise;

    public SnapshotWriter(SnapshotMeta snapshotMeta,String path, AsyncFile asyncFile) {
        this.snapshotMeta=snapshotMeta;
        this.path=path;
        this.asyncFile = asyncFile;
        this.promise=Promise.promise();
    }

    public void write(Buffer buffer){
        offset+=buffer.length();
        asyncFile.write(buffer.copy(), ar->{
            if (!ar.succeeded()) {
                logger.error("write snapshot failed",ar.cause());
                end();
            }
        });
    }

    public Future<Void> end(){

        Promise<Void> promise=Promise.promise();
        end=true;
        asyncFile.end(ar->{
            if (ar.succeeded()){
                this.promise.complete(promise);
            }else{
                this.promise.tryFail(ar.cause());
                promise.tryFail(ar.cause());
            }
        });
        return promise.future();
    }

    Future<Promise<Void>> future(){
        return promise.future();
    }

    public SnapshotMeta getSnapshotMeta() {
        return snapshotMeta;
    }

    public String getPath() {
        return path;
    }


    public int getOffset() {
        return offset;
    }

    public boolean isEnd() {
        return end;
    }
}
