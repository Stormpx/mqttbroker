package com.stormpx.store.rocksdb;

import com.stormpx.store.MessageObj;
import com.stormpx.store.MessageStore;
import com.stormpx.store.ObjCodec;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

import java.util.HashMap;
import java.util.Map;

public class RocksDBMessageStore implements MessageStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBMessageStore.class);

    private ColumnFamilyHandle MESSAGE_COLUMN_FAMILY;
    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBMessageStore(Vertx vertx) {
        this.vertx = vertx;
        this.rocksDB=Db.db();
        this.MESSAGE_COLUMN_FAMILY=Db.messageColumnFamily();
    }


    @Override
    public Future<Map<String, String>> retainMap() {
        Promise<Map<String,String>> promise=Promise.promise();
        vertx.executeBlocking(p->{
            String prefix="retain-";
            Map<String,String> map=new HashMap<>();
            RocksIterator rocksIterator = rocksDB.newIterator(MESSAGE_COLUMN_FAMILY,new ReadOptions().setPrefixSameAsStart(true));
            for (rocksIterator.seek(prefix.getBytes());rocksIterator.isValid();rocksIterator.next()){
                String topic = new String(rocksIterator.key()).substring(prefix.length());
                String id = new String(rocksIterator.value());
                map.put(topic,id);
            }
            rocksIterator.close();
            p.complete(map);
        },promise);
        return promise.future();
    }

    @Override
    public Future<MessageObj> get(String id) {
        Promise<MessageObj> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                byte[] bytes = rocksDB.get(MESSAGE_COLUMN_FAMILY,id.getBytes());
                MessageObj messageObj = new ObjCodec().decodeMessageObj(Buffer.buffer(bytes));
                p.complete(messageObj);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },false,promise);
        return promise.future();
    }

    @Override
    public void set(String id, MessageObj messageObj) {
        vertx.executeBlocking(p->{
            try {
                Buffer buffer = new ObjCodec().encodeMessageObj(messageObj);
                rocksDB.put(MESSAGE_COLUMN_FAMILY,Db.asyncWriteOptions(),id.getBytes(),buffer.getBytes());
            } catch (RocksDBException e) {
                logger.error("set id:{} messageObj:{} fail",e,id,messageObj);
            }
        },null);
    }

    @Override
    public void del(String id) {
        vertx.executeBlocking(p->{
            delete(id);
        },null);
    }

    private void delete(String id){
        try {
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.delete(MESSAGE_COLUMN_FAMILY,id.getBytes());
            writeBatch.delete(MESSAGE_COLUMN_FAMILY,("refcnt-"+id).getBytes());
            rocksDB.write(Db.asyncWriteOptions(),writeBatch);
        } catch (RocksDBException e) {
            logger.error("del id:{} fail",id);
        }
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
                    rocksDB.delete(MESSAGE_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes());
                }else{
                    //put
                    rocksDB.put(MESSAGE_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),id.getBytes());
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
                    byte[] value = rocksDB.get(MESSAGE_COLUMN_FAMILY,keyBytes);
                    int count =0;
                    if (value != null) {
                        count = Buffer.buffer(value).getInt(0);
                    }
                    count += d;
                    r=count;
                    if (r<=0){
//                        System.out.println(++i);
                        delete(id);
                    }else{
                        rocksDB.put(MESSAGE_COLUMN_FAMILY,Db.asyncWriteOptions(),keyBytes, Buffer.buffer().appendInt(count).getBytes());
                    }
                }
                p.complete(r);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }
}
