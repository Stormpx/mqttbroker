package com.stormpx.store.rocksdb;

import com.stormpx.cluster.LogEntry;
import com.stormpx.cluster.snapshot.SnapshotMeta;
import com.stormpx.kit.FileUtil;
import com.stormpx.store.ClusterDataStore;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.Comparator;
import java.util.stream.Collectors;

public class RocksDBClusterDataStore implements ClusterDataStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBClusterDataStore.class);

    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBClusterDataStore(Vertx vertx,String dir,String id) throws RocksDBException {
        this.vertx=vertx;
        String path = Paths.get(dir).normalize().toString() + "/meta_data/"+id;
        FileUtil.create(new File(path));
        this.rocksDB=RocksDB.open(path);
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
    public void saveSnapshotMeta(SnapshotMeta snapshotMeta) {
        vertx.executeBlocking(p->{
            try {
                rocksDB.put("snapshotMeta".getBytes(),snapshotMeta.encode().getBytes());
            } catch (RocksDBException e) {
                logger.error("save snapshotMeta fail",e);
            }
        },ar->{

        });
    }

    @Override
    public Future<SnapshotMeta> getSnapshotMeta() {
        Promise<SnapshotMeta> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                byte[] value = rocksDB.get("snapshotMeta".getBytes());
                p.tryComplete(value==null?null:SnapshotMeta.decode(Buffer.buffer(value)));
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
            try {
                List<byte[]> keyList=new ArrayList<>();
                int s=start;
                while (s<end){
                    keyList.add(("log-"+s).getBytes());
                    s++;
                }
                Map<byte[], byte[]> map = rocksDB.multiGet(keyList);
                p.complete(map.values().stream().map(bytes -> LogEntry.decode(0,Buffer.buffer(bytes)))
                        .sorted(Comparator.comparingInt(LogEntry::getIndex))
                        .collect(Collectors.toList()));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
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
                RocksIterator rocksIterator = rocksDB.newIterator();
                WriteBatch batch = new WriteBatch();
                for (rocksIterator.seek("log".getBytes());rocksIterator.isValid();rocksIterator.next()){
                    String key = new String(rocksIterator.key());
                    if (!key.startsWith("log"))
                        break;
                    String n = key.substring(4);
                    int sn = Integer.parseInt(n);
                    if (sn>=start&&sn<end){
                        batch.remove(rocksIterator.key());
                    }
                }
                rocksIterator.close();
                rocksDB.write(new WriteOptions().setSync(false),batch);
            } catch (RocksDBException e) {
                logger.error("del log fail",e);
            }
            p.complete();
        },ar->{

        });
    }
}
