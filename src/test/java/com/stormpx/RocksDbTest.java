package com.stormpx;

import com.stormpx.store.rocksdb.Db;
import io.netty.util.internal.ThreadLocalRandom;
import io.vertx.core.buffer.Buffer;
import org.rocksdb.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class RocksDbTest {
    public static void main(String[] args) throws RocksDBException {


        long l = System.currentTimeMillis();
        Db.initialize("D:\\JavaProject\\mqttbroker\\mqtt3");
        RocksDB rocksDB = Db.db();

        RocksIterator rocksIterator = rocksDB.newIterator(Db.messageColumnFamily());
        rocksIterator.seekToFirst();
//        rocksIterator.seek("refcnt".getBytes());
        while (rocksIterator.isValid()){
            System.out.println(new String(rocksIterator.key()));

            rocksIterator.next();
        }

        /*for (int i = 0; i < 25*10000; i++) {
            byte[] bytes = randomString().getBytes();
            byte[] bytes1 = new byte[4096];
            rocksDB.put(bytes, bytes1);
        }
        System.out.println(rocksDB.get("fuck".getBytes())==null);*/

        System.out.println(System.currentTimeMillis()-l);
    }
    private static String randomString(){
        ThreadLocalRandom random = ThreadLocalRandom.current();
        StringBuilder sb=new StringBuilder(7);
        for (int i = 0; i < 7; i++) {
            char c = (char) (random.nextInt(0, 25) + 65);
            sb.append(c);
        }
        return sb.toString();
    }

}
