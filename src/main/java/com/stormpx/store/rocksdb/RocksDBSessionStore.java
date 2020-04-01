package com.stormpx.store.rocksdb;

import com.stormpx.kit.FileUtil;
import com.stormpx.kit.J;
import com.stormpx.store.SessionObj;
import com.stormpx.store.SessionStore;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

import java.io.File;
import java.nio.file.Paths;
import java.util.*;
import java.util.stream.Collectors;

public class RocksDBSessionStore implements SessionStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBSessionStore.class);
    private ColumnFamilyHandle SESSION_COLUMN_FAMILY;
    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBSessionStore(Vertx vertx) {
        this.vertx=vertx;
        this.rocksDB=Db.db();
        this.SESSION_COLUMN_FAMILY=Db.sessionColumnFamily();
    }


    @Override
    public Future<SessionObj> get(String clientId) {
        Promise<SessionObj> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String expiryTimestampKey=clientId+"-expiryTimestamp";
                String willKey=clientId+"-will";
                String subscribeKey=clientId+"-subscribe";
                String linkPrefix=clientId+"-link-";
                String packetIdPrefix=clientId+"-packetId-";
                SessionObj sessionObj = new SessionObj(clientId);

                byte[] expiryTimestampValue = rocksDB.get(SESSION_COLUMN_FAMILY,expiryTimestampKey.getBytes());
                if (expiryTimestampValue!=null){
                    sessionObj.setExpiryTimestamp(Buffer.buffer(expiryTimestampValue).getLong(0));
                }
                byte[] willValue = rocksDB.get(SESSION_COLUMN_FAMILY,willKey.getBytes());
                if (willValue!=null){
                    sessionObj.setWill(Buffer.buffer(willValue).toJsonObject());
                }
                byte[] subscribeValue = rocksDB.get(SESSION_COLUMN_FAMILY,subscribeKey.getBytes());
                if (subscribeValue!=null){
                    sessionObj.addTopicSubscription(Buffer.buffer(subscribeValue).toJsonArray());
                }
                RocksIterator linkRocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);
                for (linkRocksIterator.seek(linkPrefix.getBytes());linkRocksIterator.isValid()&&new String(linkRocksIterator.key()).startsWith(linkPrefix);linkRocksIterator.next()){
                    byte[] value = linkRocksIterator.value();
                    JsonObject link = Buffer.buffer(value).toJsonObject();
                    Integer packetId = link.getInteger("packetId");
                    String id = link.getString("id");
                    if (packetId!=null){
                        sessionObj.addMessageLink(packetId,link);
                    }else{
                        sessionObj.addPendingId(id,link);
                    }
                }

                linkRocksIterator.close();

                RocksIterator packetIdRocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);
                for (packetIdRocksIterator.seek(packetIdPrefix.getBytes());
                     packetIdRocksIterator.isValid()&&new String(packetIdRocksIterator.key()).startsWith(packetIdPrefix);
                     packetIdRocksIterator.next()){
                    byte[] value = packetIdRocksIterator.value();
                    sessionObj.addPacketId(Buffer.buffer(value).getInt(0));
                }
                packetIdRocksIterator.close();
                p.complete(sessionObj);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },false,promise);

        return promise.future();
    }

    @Override
    public Future<Void> save(SessionObj sessionObj) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String clientId = sessionObj.getClientId();
                WriteBatch batch = new WriteBatch();

                RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);

                for (rocksIterator.seek(clientId.getBytes());
                     rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(clientId);
                     rocksIterator.next()){
                    batch.remove(SESSION_COLUMN_FAMILY,rocksIterator.key());
                    rocksIterator.next();
                }
                rocksIterator.close();

                if (sessionObj.getExpiryTimestamp()!=null){
                    String key=clientId+"-expiryTimestamp";
                    byte[] value = Buffer.buffer().appendLong(sessionObj.getExpiryTimestamp()).getBytes();
                    batch.put(SESSION_COLUMN_FAMILY,key.getBytes(),value);
                }
                if (sessionObj.getWill()!=null){
                    String key=clientId+"-will";
                    batch.put(SESSION_COLUMN_FAMILY,key.getBytes(),sessionObj.getWill().toBuffer().getBytes());
                }
                if (sessionObj.getTopicSubscriptions()!=null){
                    String key=clientId+"-subscribe";
                    Buffer value = sessionObj.getTopicSubscriptions().toBuffer();
                    batch.put(SESSION_COLUMN_FAMILY,key.getBytes(),value.getBytes());
                }
                if (sessionObj.getMessageLinkMap()!=null){
                    for (Map.Entry<Integer, JsonObject> entry : sessionObj.getMessageLinkMap().entrySet()) {
                        Integer k = entry.getKey();
                        JsonObject v = entry.getValue();
                        String key = clientId + "-link-" + k;
                        batch.put(SESSION_COLUMN_FAMILY,key.getBytes(), v.toBuffer().getBytes());
                    }

                }
                if (sessionObj.getPendingMessage()!=null){
                    for (Map.Entry<String, JsonObject> entry : sessionObj.getPendingMessage().entrySet()) {
                        String k = entry.getKey();
                        JsonObject v = entry.getValue();
                        String key = clientId + "-link-" + k;
                        batch.put(SESSION_COLUMN_FAMILY,key.getBytes(), v.toBuffer().getBytes());
                    }
                }
                if (sessionObj.getPacketIdSet()!=null){
                    for (Integer packetId : sessionObj.getPacketIdSet()) {
                        String key = clientId + "-packetId-" + packetId;
                        batch.put(SESSION_COLUMN_FAMILY,key.getBytes(), Buffer.buffer().appendInt(packetId).getBytes());
                    }
                }
                rocksDB.write(Db.asyncWriteOptions(),batch);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("save client:{} sessionObj fail",e,sessionObj.getClientId());
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    private void delClient(String clientId) throws RocksDBException {
        RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);
        WriteBatch batch = new WriteBatch();
        for (rocksIterator.seek(clientId.getBytes());
            rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(clientId);
            rocksIterator.next()) {

            batch.delete(SESSION_COLUMN_FAMILY,rocksIterator.key());
        }
        rocksIterator.close();

        rocksDB.write(Db.asyncWriteOptions(),batch);
    }

    @Override
    public Future<Void> del(String clientId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {

                delClient(clientId);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("del session {} fail",e,clientId);
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<Void> setExpiryTimestamp(String clientId, Long expiryTimestamp) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-expiryTimestamp";
                byte[] value = Buffer.buffer().appendLong(expiryTimestamp).getBytes();
                rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),value);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("set client: {} expiryTimestamp :{} fail",e,clientId,expiryTimestamp);
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<Long> getExpiryTimestamp(String clientId) {
        Promise<Long> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-expiryTimestamp";
                byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                if (value!=null)
                    p.complete(Buffer.buffer(value).getLong(0));
                else
                    p.complete();

            } catch (RocksDBException e) {
                logger.error("get client:{} expiryTimestamp failed",e,clientId);
                throw new RuntimeException(e);
            }
        },false,promise);

        return promise.future();
    }

    @Override
    public Future<Void> addLink(String clientId, JsonObject link) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                WriteBatch writeBatch = new WriteBatch();
                String id = link.getString("id");
                writeBatch.remove(SESSION_COLUMN_FAMILY,(clientId+"-link-"+id).getBytes());
                Integer packetId = link.getInteger("packetId");
                if (packetId==null){
                    String key = clientId + "-link-" + id;
                    writeBatch.put(SESSION_COLUMN_FAMILY,key.getBytes(), link.toBuffer().getBytes());
                }else {
                    String key = clientId + "-link-" + packetId;
                    writeBatch.put(SESSION_COLUMN_FAMILY,key.getBytes(), link.toBuffer().getBytes());
                }
                rocksDB.write(Db.asyncWriteOptions(),writeBatch);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("add client:{} link :{} fail",e,clientId,link.encode());
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<String> release(String clientId, int packetId) {
        Promise<String> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key = clientId + "-link-" + packetId;
                byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                if (value==null){
                    p.complete();
                    return;
                }
                rocksDB.delete(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes());
                JsonObject link = Buffer.buffer(value).toJsonObject();
                p.complete(link.getString("id"));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<String> receive(String clientId, int packetId) {
        Promise<String> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key = clientId + "-link-" + packetId;
                byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),new JsonObject().put("clientId",clientId).put("packetId",packetId).toBuffer().getBytes());
                JsonObject link = Buffer.buffer(value).toJsonObject();
                p.complete(link.getString("id"));
            } catch (RocksDBException e) {
                logger.error("set client:{} receive packetId:{} failed",e,clientId,packetId);
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<List<JsonObject>> links(String clientId) {

        Promise<List<JsonObject>> promise=Promise.promise();
        vertx.executeBlocking(p->{
            String prefix=clientId+"-link-";
            RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);
            rocksIterator.seek(prefix.getBytes());
            List<JsonObject> list=new ArrayList<>();
            for (rocksIterator.seek(prefix.getBytes());rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(prefix);rocksIterator.next()){
                byte[] value = rocksIterator.value();
                list.add(Buffer.buffer(value).toJsonObject());
                rocksIterator.next();
            }
            rocksIterator.close();


            p.complete(list);
        },false,promise);

        return promise.future();
    }

    @Override
    public Future<Void> addPacketId(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-packetId-"+packetId;
                rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),Buffer.buffer().appendInt(packetId).getBytes());
                p.complete();
            } catch (RocksDBException e) {
                logger.error("add client:{} packetId :{} failed",e,clientId,packetId);
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<List<Integer>> packetId(String clientId) {

        Promise<List<Integer>> promise=Promise.promise();
        vertx.executeBlocking(p->{
            String prefix=clientId+"-packetId-";
            RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);

            List<Integer> list=new ArrayList<>();
            for (rocksIterator.seek(prefix.getBytes());
                rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(prefix);
                rocksIterator.next()) {
                byte[] value = rocksIterator.value();
                list.add(Buffer.buffer(value).getInt(0));
                rocksIterator.next();
            }
            rocksIterator.close();
            p.complete(list);
        },false,promise);

        return promise.future();
    }

    @Override
    public Future<Void> removePacketId(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-packetId-"+packetId;
                rocksDB.delete(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes());
                p.complete();
            } catch (RocksDBException e) {
                logger.error("del client:{} packetId packetId: {} failed",e,clientId,packetId);
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<Void> saveWill(String clientId, JsonObject will) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-will";
                rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),will.toBuffer().getBytes());
                p.complete();
            } catch (RocksDBException e) {
                logger.error("del client:{} will :{} failed",e,clientId,will.encodePrettily());
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<JsonObject> getWill(String clientId) {
        Promise<JsonObject> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-will";
                byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                if (value==null)
                    p.complete();
                else
                    p.complete(Buffer.buffer(value).toJsonObject());
            } catch (RocksDBException e) {
                logger.error("get client:{} will failed",e,clientId);
                throw new RuntimeException(e);
            }
        },false,promise);
        return promise.future();
    }

    @Override
    public Future<Void> delWill(String clientId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-will";
                rocksDB.delete(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes());
            } catch (RocksDBException e) {
                logger.error("del client:{} will failed",e,clientId);
                throw new RuntimeException(e);
            }
            p.complete();
        },promise);
        return promise.future();
    }

    @Override
    public Future<Void> addSubscription(String clientId, JsonArray jsonArray) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-subscribe";
                byte[] v = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                JsonArray array = Optional.ofNullable(v).map(bytes -> Buffer.buffer(bytes).toJsonArray()).orElseGet(JsonArray::new);

                Set<String> topicFilter = J.toJsonStream(jsonArray).map(json -> json.getString("topicFilter")).collect(Collectors.toSet());
                JsonArray filter = J.toJsonStream(array).filter(json -> !topicFilter.contains(json.getString("topicFilter"))).collect(J.toJsonArray());
                Buffer value = filter.addAll(jsonArray).toBuffer();
                rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),value.getBytes());
                p.complete();
            } catch (RocksDBException e) {
                logger.error("add client:{} subscription :{} failed",e,clientId,jsonArray.encode());
                throw new RuntimeException(e);
            }

        },promise);
        return promise.future();
    }

    @Override
    public Future<JsonArray> fetchSubscription(String clientId) {
        Promise<JsonArray> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-subscribe";
                byte[] bytes = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                if (bytes!=null) {
                    JsonArray array = Buffer.buffer(bytes).toJsonArray();
                    p.complete(array);
                }else{
                    p.complete();
                }
            } catch (RocksDBException e) {
                logger.error("fetch client:{} subscription failed",e,clientId);
                throw new RuntimeException(e);
            }
        },false,promise);
        return promise.future();
    }

    @Override
    public Future<Void> deleteSubscription(String clientId, List<String> topics) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-subscribe";
                byte[] bytes = rocksDB.get(SESSION_COLUMN_FAMILY,key.getBytes());
                if (bytes!=null) {
                    JsonArray array = Buffer.buffer(bytes).toJsonArray();
                    Set<String> topicSet = Set.copyOf(topics);
                    JsonArray filter = J.toJsonStream(array).filter(json -> !topicSet.contains(json.getString("topicFilter"))).collect(J.toJsonArray());
                    Buffer value = filter.toBuffer();
                    rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key.getBytes(),value.getBytes());
                }
                p.complete();
            } catch (RocksDBException e) {
                logger.error("del client:{} subscription :{} failed",e,clientId,topics);
                throw new RuntimeException(e);
            }

        },promise);
        return promise.future();
    }
}
