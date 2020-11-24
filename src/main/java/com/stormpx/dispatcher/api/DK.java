package com.stormpx.dispatcher.api;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;


public class DK {

    protected Vertx vertx;

    public DK(Vertx vertx) {
        this.vertx = vertx;
    }

    protected <T> Future<T> request(String address, Object message){
        Promise<T> promise=Promise.promise();
        vertx.eventBus().<T>request(address,message,ar->{
            if (ar.succeeded()){
                promise.tryComplete(ar.result().body());
            }else{
                promise.tryFail(ar.cause());
            }
        });
        return promise.future();
    }

}
