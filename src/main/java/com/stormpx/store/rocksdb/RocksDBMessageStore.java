package com.stormpx.store.rocksdb;

import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.kit.Codec;
import com.stormpx.store.BlockStore;
import com.stormpx.store.MessageStore;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.parsetools.impl.JsonParserImpl;
import org.rocksdb.*;

import java.util.HashMap;
import java.util.Map;

public class RocksDBMessageStore extends BlockStore implements MessageStore {
    private final static Logger logger = LoggerFactory.getLogger(RocksDBMessageStore.class);

    private ColumnFamilyHandle MESSAGE_COLUMN_FAMILY;
    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBMessageStore(Vertx vertx) {
        super(vertx);
        this.vertx = vertx;
        this.rocksDB = Db.db();
        this.MESSAGE_COLUMN_FAMILY = Db.messageColumnFamily();

    }


    @Override
    public Future<Map<String, String>> retainMap() {
        return readOps(() -> {
            String prefix = "retain-";
            Map<String, String> map = new HashMap<>();
            RocksIterator rocksIterator = rocksDB.newIterator(MESSAGE_COLUMN_FAMILY, new ReadOptions().setPrefixSameAsStart(true));
            for (rocksIterator.seek(prefix.getBytes()); rocksIterator.isValid(); rocksIterator.next()) {
                String topic = new String(rocksIterator.key()).substring(prefix.length());
                String id = new String(rocksIterator.value());
                map.put(topic, id);
            }
            rocksIterator.close();
            return map;
        });
    }

    @Override
    public Future<DispatcherMessage> get(String id) {
        return readOps(() -> {
            byte[] bytes = rocksDB.get(MESSAGE_COLUMN_FAMILY, id.getBytes());
            return bytes == null ? null : Codec.decodeDispatcherMessage(Buffer.buffer(bytes));
        });
    }

    @Override
    public void save(String id, DispatcherMessage message) {
        writeOps(() -> {
            rocksDB.put(MESSAGE_COLUMN_FAMILY, Db.asyncWriteOptions(), id.getBytes(), Codec.encode(message));
            return null;
        });
    }



    @Override
    public Future<Void> del(String id) {
        return writeOps(() -> {
            delete(id);
            return null;
        });
    }

    private void delete(String id) {
        try {
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.delete(MESSAGE_COLUMN_FAMILY, id.getBytes());
            writeBatch.delete(MESSAGE_COLUMN_FAMILY, ("refcnt-" + id).getBytes());
            rocksDB.write(Db.asyncWriteOptions(), writeBatch);
        } catch (RocksDBException e) {
            logger.error("del id:{} fail", id);
        }

    }

    @Override
    public Future<String> putRetain(String topic, String id) {
        return writeOps(() -> {
            String key = "retain-" + topic;
            byte[] value = rocksDB.get(key.getBytes());
            if (id == null) {
                //del
                rocksDB.delete(MESSAGE_COLUMN_FAMILY, Db.asyncWriteOptions(), key.getBytes());
            } else {
                //put
                rocksDB.put(MESSAGE_COLUMN_FAMILY, Db.asyncWriteOptions(), key.getBytes(), id.getBytes());
            }
            return value == null ? null : new String(value);
        });
    }



    @Override
    public Future<Integer> getRef(String id) {
        return readOps(() -> {
            String key = "refcnt-" + id;
            byte[] keyBytes = key.getBytes();
            byte[] value = rocksDB.get(MESSAGE_COLUMN_FAMILY, keyBytes);
            return value==null?0:Buffer.buffer(value).getInt(0);
        });
    }

    @Override
    public void saveRef(String id, int d) {
        writeOps(()->{
            String key = "refcnt-" + id;
            byte[] keyBytes = key.getBytes();
            rocksDB.put(MESSAGE_COLUMN_FAMILY, Db.asyncWriteOptions(), keyBytes, Buffer.buffer().appendInt(d).getBytes());
            return null;
        });

    }

}
