package com.stormpx;

import io.vertx.core.Vertx;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

public class Main {
    public static void main(String[] args) throws RocksDBException {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");

        /*Vertx vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        MqttBroker.start(vertx,args)
                .setHandler(ar->{
                    if (ar.failed()) {
                        ar.cause().printStackTrace();
                        vertx.close();
                    }
                });*/

        RocksDB rocksDB = RocksDB.open("D:\\rdb");

        rocksDB.put(new WriteOptions(),"key1".getBytes(),"value".getBytes());
        rocksDB.put(new WriteOptions(),"key3".getBytes(),"value".getBytes());
        rocksDB.put(new WriteOptions(),"huo".getBytes(),"value".getBytes());
        rocksDB.put(new WriteOptions(),"key2".getBytes(),"value".getBytes());

        byte[] bytes = rocksDB.get("123".getBytes());
        System.out.println(bytes==null);

        RocksIterator rocksIterator = rocksDB.newIterator(new ReadOptions());
        rocksIterator.seek("key".getBytes());
        while (rocksIterator.isValid()){
            System.out.println(new String(rocksIterator.key()));
            rocksIterator.next();
        }
        System.out.println();
        rocksDB.deleteRange("key2".getBytes(),"key3".getBytes());

        rocksIterator = rocksDB.newIterator(new ReadOptions());
        rocksIterator.seekToFirst();
        while (rocksIterator.isValid()){
            System.out.println(new String(rocksIterator.key()));
            rocksIterator.next();
        }
    }

}
