package com.stormpx;

import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

import java.io.File;

public class Main {
    public static void main(String[] args) throws RocksDBException {
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
