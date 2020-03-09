package com.stormpx.cluster.snapshot;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class SnapshotWriter {
    private final static Logger logger= LoggerFactory.getLogger(SnapshotWriter.class);
    private String id;
    private int num;
    private AsyncFile asyncFile;
    private Promise<Void> promise;

    public SnapshotWriter(String id, AsyncFile asyncFile) {
        this.id=id;
        this.asyncFile = asyncFile;
        this.promise=Promise.promise();
    }

    public void write(Buffer buffer){
        num++;
        asyncFile.write(buffer, ar->{
            if (!ar.succeeded()) {
                logger.error("writeSnapshot snapshot failed",ar.cause());
                end();
            }
        });
    }

    public void end(){
        asyncFile.end(promise);
    }

    public Future<Void> future(){
        return promise.future();
    }

    public String getId() {
        return id;
    }

    public int getNum() {
        return num;
    }
}
