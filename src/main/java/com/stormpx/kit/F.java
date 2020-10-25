package com.stormpx.kit;

import io.vertx.core.Future;

public class F {

    public static <T> Future<T> failWhenNull(T t){
        if (t==null)
            return Future.failedFuture(new NullPointerException("obj is null"));
        return Future.succeededFuture(t);
    }

}
