package com.stormpx.store;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;

public class BlockStore {
    private Vertx vertx;

    public BlockStore(Vertx vertx) {
        this.vertx = vertx;
    }



    protected <T> Future<T> readOps(StoreOps<T> readOps){
        return ops(false,readOps);
    }

    protected <T> Future<T> writeOps(StoreOps<T> writeOps){

       return ops(true,writeOps);
    }

    protected <T> Future<T> ops(boolean order,StoreOps<T> ops){

        Promise<T> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                promise.tryComplete(ops.ops());
            } catch (Throwable throwable) {
                promise.tryFail(throwable);
            }
        },order,promise);
        return promise.future();
    }

}
