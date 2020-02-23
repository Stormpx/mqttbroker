package com.stormpx.store.rocksdb;

import com.stormpx.cluster.LogEntry;
import com.stormpx.store.ClusterDataStore;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class RocksDBClusterDataStore implements ClusterDataStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBClusterDataStore.class);

    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBClusterDataStore(Vertx vertx,String dir) throws RocksDBException {
        this.vertx=vertx;
        String path = Paths.get(dir).normalize().toString() + "/meta_data";
        this.rocksDB=RocksDB.open(path);
    }

    @Override
    public void setRequestId(int requestId) {
        vertx.executeBlocking(p->{
            try {
                rocksDB.put("requestId".getBytes(),String.valueOf(requestId).getBytes());
            } catch (RocksDBException e) {
                e.printStackTrace();
                logger.error("set requestId fail",e);
            }
        },ar->{

        });

    }

    @Override
    public Future<Integer> requestId() {
        Promise<Integer> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                byte[] value = rocksDB.get("requestId".getBytes());
                p.tryComplete(Integer.valueOf(new String(value)));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }

    @Override
    public void saveState(JsonObject state) {

        vertx.executeBlocking(p->{
            try {
                rocksDB.put("state".getBytes(),state.toBuffer().getBytes());
            } catch (RocksDBException e) {
                logger.error("save state fail",e);
            }
        },ar->{

        });

    }

    @Override
    public Future<JsonObject> getState() {

        Promise<JsonObject> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                byte[] value = rocksDB.get("state".getBytes());
                JsonObject jsonObject = (JsonObject) Json.decodeValue(Buffer.buffer(value));
                p.complete(jsonObject);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }

    @Override
    public Future<List<LogEntry>> logs() {
        Promise<List<LogEntry>> promise=Promise.promise();
        vertx.executeBlocking(p->{
            RocksIterator rocksIterator = rocksDB.newIterator();
            rocksIterator.seekToFirst();
            List<LogEntry> logEntries=new ArrayList<>();
            while (rocksIterator.isValid()){
                String log = new String(rocksIterator.key());
                if (log.startsWith("log")){
                    LogEntry logEntry = Json.decodeValue(Buffer.buffer(rocksIterator.value()), LogEntry.class);
                    logEntries.add(logEntry);
                }
            }
            p.complete(logEntries);
        },promise);

        return promise.future();
    }

    @Override
    public void saveLog(LogEntry logEntry) {

        vertx.executeBlocking(p->{
            try {
                String key="log-"+logEntry.getIndex();
                Buffer value = Json.encodeToBuffer(logEntry);
                rocksDB.put(key.getBytes(),value.getBytes());
            } catch (RocksDBException e) {
                logger.error("save log fail",e);
            }
            p.complete();
        },ar->{

        });
    }

    @Override
    public void delLog(int start, int end) {

        vertx.executeBlocking(p->{
            try {
                rocksDB.deleteRange(("log-"+start).getBytes(),("log-"+end).getBytes());
            } catch (RocksDBException e) {
                logger.error("del log fail",e);
            }
            p.complete();
        },ar->{

        });
    }
}
