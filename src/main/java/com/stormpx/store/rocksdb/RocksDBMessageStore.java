package com.stormpx.store.rocksdb;

import com.stormpx.store.MessageObj;
import com.stormpx.store.MessageStore;
import com.stormpx.store.ObjCodec;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksDBMessageStore implements MessageStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBMessageStore.class);
    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBMessageStore(Vertx vertx,String dir) throws RocksDBException {
        this.vertx = vertx;
        String path = Paths.get(dir).normalize().toString() + "/message";
        create(new File(path));
        this.rocksDB=RocksDB.open(path);
    }

    private void create(File file){
        if (!file.getParentFile().exists())
            create(file.getParentFile());
        file.mkdir();
    }

    @Override
    public Future<Map<String, String>> retainMap() {
        Promise<Map<String,String>> promise=Promise.promise();
        vertx.executeBlocking(p->{
            String prefix="retain-";
            RocksIterator rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(prefix.getBytes());
            Map<String,String> map=new HashMap<>();
            while (rocksIterator.isValid()){
                String topic = new String(rocksIterator.key()).substring(0, prefix.length());
                String id = new String(rocksIterator.value());
                map.put(topic,id);
                rocksIterator.next();
            }
            p.complete(map);
        },promise);
        return promise.future();
    }

    @Override
    public Future<MessageObj> get(String id) {
        Promise<MessageObj> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                byte[] bytes = rocksDB.get(id.getBytes());
                MessageObj messageObj = new ObjCodec().decodeMessageObj(Buffer.buffer(bytes));
                p.complete(messageObj);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public void set(String id, MessageObj messageObj) {
        vertx.executeBlocking(p->{
            try {
                Buffer buffer = new ObjCodec().encodeMessageObj(messageObj);
                rocksDB.put(id.getBytes(),buffer.getBytes());
            } catch (RocksDBException e) {
                logger.error("set id:{} messageObj:{} fail",e,id,messageObj);
            }
        },null);
    }

    @Override
    public void del(String id) {
        vertx.executeBlocking(p->{
            try {
                rocksDB.delete(id.getBytes());
            } catch (RocksDBException e) {
                logger.error("del id:{} fail",id);
            }
        },null);
    }

    @Override
    public Future<String> putRetain(String topic, String id) {
        Promise<String > promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key="retain-"+topic;
                byte[] value = rocksDB.get(key.getBytes());
                if (id==null){
                    //del
                    rocksDB.delete(key.getBytes());
                }else{
                    //put
                    rocksDB.put(key.getBytes(),id.getBytes());
                }
                p.complete(value==null?null:new String(value));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<Integer> addAndGetRefCnt(String id, int d) {
        Promise<Integer> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key="refcnt-"+id;
                Integer r=null;
                synchronized(key.intern()) {
                    byte[] keyBytes = key.getBytes();
                    byte[] value = rocksDB.get(keyBytes);
                    if (value == null) {
                        p.complete();
                        return;
                    }
                    int count = Buffer.buffer(value).getInt(0);
                    count -= 1;
                    r=count;
                    rocksDB.put(keyBytes, Buffer.buffer().appendInt(count).getBytes());
                }
                p.complete(r);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }
}
