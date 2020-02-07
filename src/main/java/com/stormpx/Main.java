package com.stormpx;

import io.vertx.core.Vertx;
import io.vertx.core.logging.LoggerFactory;

public class Main {
    public static void main(String[] args) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");

        Vertx vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        MqttBroker.start(vertx,args)
                .setHandler(ar->{
                    if (ar.failed()) {
                        ar.cause().printStackTrace();
                        vertx.close();
                    }
                });

    }

}
