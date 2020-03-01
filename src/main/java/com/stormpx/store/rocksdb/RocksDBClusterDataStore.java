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
import org.rocksdb.WriteOptions;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class RocksDBClusterDataStore implements ClusterDataStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBClusterDataStore.class);

    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBClusterDataStore(Vertx vertx,String dir,String id) throws RocksDBException {
        this.vertx=vertx;
        String path = Paths.get(dir).normalize().toString() + "/meta_data/"+id;
        create(new File(path));
        this.rocksDB=RocksDB.open(path);
    }

    private void create(File file){
        if (!file.getParentFile().exists())
            create(file.getParentFile());
        file.mkdir();
    }

    @Override
    public void setRequestId(int requestId) {
        vertx.executeBlocking(p->{
            try {
                rocksDB.put("requestId".getBytes(),Buffer.buffer(4).appendInt(requestId).getBytes());
            } catch (RocksDBException e) {
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
                p.tryComplete(value==null?1:Buffer.buffer(value).getInt(0));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }

    @Override
    public void saveIndex(int firstIndex, int lastIndex) {
        vertx.executeBlocking(p->{
            try {
                rocksDB.put("index".getBytes(),new JsonObject().put("firstIndex",firstIndex).put("lastIndex",lastIndex).toBuffer().getBytes());
            } catch (RocksDBException e) {
                logger.error("set requestId fail",e);
            }
        },ar->{

        });
    }

    @Override
    public Future<JsonObject> getIndex() {
        Promise<JsonObject> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                byte[] value = rocksDB.get("index".getBytes());
                p.tryComplete(value==null?null:Buffer.buffer(value).toJsonObject());
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
                if (value==null){
                    p.complete();
                    return;
                }
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
                    LogEntry logEntry = LogEntry.decode(0,Buffer.buffer(rocksIterator.value()));
                    logEntries.add(logEntry);
                }
                rocksIterator.next();
            }
            rocksIterator.close();
            p.complete(logEntries);
        },promise);

        return promise.future();
    }

    @Override
    public Future<List<LogEntry>> getLogs(int start, int end){

        Promise<List<LogEntry>> promise=Promise.promise();
        vertx.executeBlocking(p->{
            RocksIterator rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(("log-"+start).getBytes());
            List<LogEntry> logEntries=new LinkedList<>();
            while (rocksIterator.isValid()){
                String log = new String(rocksIterator.key());
                if (log.startsWith("log")){
                    Integer logIndex = Integer.valueOf(log.substring(3));
                    if (logIndex<end) {
                        LogEntry logEntry = LogEntry.decode(0, Buffer.buffer(rocksIterator.value()));
                        logEntries.add(logEntry);
                    }else{
                        break;
                    }
                }
                rocksIterator.next();
            }
            rocksIterator.close();
            p.complete(logEntries);
        },promise);

        return promise.future();
    }

    @Override
    public void saveLog(LogEntry logEntry) {

        vertx.executeBlocking(p->{
            try {
                String key="log-"+logEntry.getIndex();
                rocksDB.put(new WriteOptions().setSync(false),key.getBytes(),logEntry.encode().getBytes());
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
                rocksDB.deleteRange(new WriteOptions().setSync(false),("log-"+start).getBytes(),("log-"+end).getBytes());
            } catch (RocksDBException e) {
                logger.error("del log fail",e);
            }
            p.complete();
        },ar->{

        });
    }
}
