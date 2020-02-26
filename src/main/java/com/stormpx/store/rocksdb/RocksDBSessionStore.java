package com.stormpx.store.rocksdb;

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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class RocksDBSessionStore implements SessionStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBSessionStore.class);

    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBSessionStore(Vertx vertx,String dir) throws RocksDBException {
        this.vertx=vertx;
        String path = Paths.get(dir).normalize().toString() + "/session/default";
        File file = new File(path);
        create(file);
        this.rocksDB=RocksDB.open(path);
    }

    private void create(File file){
        if (!file.getParentFile().exists())
            create(file.getParentFile());
        file.mkdir();
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

                byte[] expiryTimestampValue = rocksDB.get(expiryTimestampKey.getBytes());
                if (expiryTimestampValue!=null){
                    sessionObj.setExpiryTimestamp(Buffer.buffer(expiryTimestampValue).getLong(0));
                }
                byte[] willValue = rocksDB.get(willKey.getBytes());
                if (willValue!=null){
                    sessionObj.setWill(Buffer.buffer(willValue).toJsonObject());
                }
                byte[] subscribeValue = rocksDB.get(subscribeKey.getBytes());
                if (subscribeValue!=null){
                    sessionObj.addTopicSubscription(Buffer.buffer(subscribeKey).toJsonArray());
                }
                RocksIterator linkRocksIterator = rocksDB.newIterator();
                linkRocksIterator.seek(linkPrefix.getBytes());
                while (linkRocksIterator.isValid()){
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

                RocksIterator packetIdRocksIterator = rocksDB.newIterator();
                packetIdRocksIterator.seek(packetIdPrefix.getBytes());
                while (packetIdRocksIterator.isValid()){
                    byte[] value = packetIdRocksIterator.value();
                    sessionObj.addPacketId(Buffer.buffer(value).getInt(0));
                }
                p.complete(sessionObj);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }

    @Override
    public Future<Void> save(SessionObj sessionObj) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String clientId = sessionObj.getClientId();
                WriteBatch batch = new WriteBatch();

                RocksIterator rocksIterator = rocksDB.newIterator();
                rocksIterator.seek(clientId.getBytes());
                while (rocksIterator.isValid()){
                    batch.remove(rocksIterator.key());
                    rocksIterator.next();
                }
                rocksIterator.close();
                if (sessionObj.getExpiryTimestamp()!=null){
                    String key=clientId+"-expiryTimestamp";
                    byte[] value = Buffer.buffer().appendLong(sessionObj.getExpiryTimestamp()).getBytes();
                    batch.put(key.getBytes(),value);
                }
                if (sessionObj.getWill()!=null){
                    String key=clientId+"-will";
                    batch.put(key.getBytes(),sessionObj.getWill().toBuffer().getBytes());
                }
                if (sessionObj.getTopicSubscriptions()!=null){
                    String key=clientId+"-subscribe";
                    Buffer value = sessionObj.getTopicSubscriptions().toBuffer();
                    batch.put(key.getBytes(),value.getBytes());
                }
                if (sessionObj.getMessageLinkMap()!=null){
                    for (Map.Entry<Integer, JsonObject> entry : sessionObj.getMessageLinkMap().entrySet()) {
                        Integer k = entry.getKey();
                        JsonObject v = entry.getValue();
                        String key = clientId + "-link-" + k;
                        batch.put(key.getBytes(), v.toBuffer().getBytes());
                    }

                }
                if (sessionObj.getPendingMessage()!=null){
                    for (Map.Entry<String, JsonObject> entry : sessionObj.getPendingMessage().entrySet()) {
                        String k = entry.getKey();
                        JsonObject v = entry.getValue();
                        String key = clientId + "-link-" + k;
                        batch.put(key.getBytes(), v.toBuffer().getBytes());
                    }
                }
                if (sessionObj.getPacketIdSet()!=null){
                    for (Integer packetId : sessionObj.getPacketIdSet()) {
                        String key = clientId + "-packetId-" + packetId;
                        batch.put(key.getBytes(), Buffer.buffer().appendInt(packetId).getBytes());
                    }
                }
                rocksDB.write(new WriteOptions(),batch);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("save client:{} sessionObj fail",e,sessionObj.getClientId());
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    private void delClient(String clientId) throws RocksDBException {
        RocksIterator rocksIterator = rocksDB.newIterator();
        rocksIterator.seek(clientId.getBytes());
        while (rocksIterator.isValid()){
            rocksDB.delete(rocksIterator.key());
            rocksIterator.next();
        }
        rocksIterator.close();
    }

    @Override
    public Future<Void> del(String clientId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                delClient(clientId);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("del session "+clientId+" fail",e);
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
                rocksDB.put(key.getBytes(),value);
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
                byte[] value = rocksDB.get(key.getBytes());
                p.complete(Long.valueOf(new String(value)));
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        },promise);

        return promise.future();
    }

    @Override
    public Future<Void> addLink(String clientId, JsonObject link) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                WriteBatch writeBatch = new WriteBatch();
                String id = link.getString("id");
                writeBatch.remove((clientId+"-link-"+id).getBytes());
                Integer packetId = link.getInteger("packetId");
                if (packetId==null){
                    String key = clientId + "-link-" + id;
                    writeBatch.put(key.getBytes(), link.toBuffer().getBytes());
                }else {
                    String key = clientId + "-link-" + packetId;
                    writeBatch.put(key.getBytes(), link.toBuffer().getBytes());
                }
                rocksDB.write(new WriteOptions(),writeBatch);
                p.complete();
            } catch (RocksDBException e) {
                logger.error("add client:{} link :{} fail",e,clientId,link.encodePrettily());
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
                byte[] value = rocksDB.get(key.getBytes());
                rocksDB.delete(key.getBytes());
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
                byte[] value = rocksDB.get(key.getBytes());
                rocksDB.put(key.getBytes(),new JsonObject().put("clientId",clientId).put("packetId",packetId).toBuffer().getBytes());
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
            RocksIterator rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(prefix.getBytes());
            List<JsonObject> list=new ArrayList<>();
            while (rocksIterator.isValid()){
                byte[] value = rocksIterator.value();
                list.add(Buffer.buffer(value).toJsonObject());
                rocksIterator.next();
            }
            rocksIterator.close();
            p.complete(list);
        },promise);

        return promise.future();
    }

    @Override
    public Future<Void> addPacketId(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-packetId-"+packetId;
                rocksDB.put(key.getBytes(),Buffer.buffer().appendInt(packetId).getBytes());
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
            RocksIterator rocksIterator = rocksDB.newIterator();
            rocksIterator.seek(prefix.getBytes());
            List<Integer> list=new ArrayList<>();
            while (rocksIterator.isValid()){
                byte[] value = rocksIterator.value();
                list.add(Buffer.buffer(value).getInt(0));
                rocksIterator.next();
            }
            rocksIterator.close();
            p.complete(list);
        },promise);

        return promise.future();
    }

    @Override
    public Future<Void> removePacketId(String clientId, int packetId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-packetId-"+packetId;
                rocksDB.delete(key.getBytes());
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
                rocksDB.put(key.getBytes(),will.toBuffer().getBytes());
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
                byte[] value = rocksDB.get(key.getBytes());
                p.complete(Buffer.buffer(value).toJsonObject());
            } catch (RocksDBException e) {
                logger.error("get client:{} will failed",e,clientId);
                throw new RuntimeException(e);
            }
        },promise);
        return promise.future();
    }

    @Override
    public Future<Void> delWill(String clientId) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-will";
                rocksDB.singleDelete(key.getBytes());
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
                byte[] bytes = rocksDB.get(key.getBytes());
                JsonArray array = Buffer.buffer(bytes).toJsonArray();

                Set<String> topicFilter = J.toJsonStream(jsonArray).map(json -> json.getString("topicFilter")).collect(Collectors.toSet());
                JsonArray filter = J.toJsonStream(array).filter(json -> !topicFilter.contains(json.getString("topicFilter"))).collect(J.toJsonArray());
                Buffer value = filter.addAll(jsonArray).toBuffer();
                rocksDB.put(key.getBytes(),value.getBytes());
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
                byte[] bytes = rocksDB.get(key.getBytes());
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
        },promise);
        return promise.future();
    }

    @Override
    public Future<Void> deleteSubscription(String clientId, List<String> topics) {
        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            try {
                String key=clientId+"-subscribe";
                byte[] bytes = rocksDB.get(key.getBytes());
                JsonArray array = Buffer.buffer(bytes).toJsonArray();
                Set<String> topicSet = Set.copyOf(topics);
                JsonArray filter = J.toJsonStream(array).filter(json -> !topicSet.contains(json.getString("topicFilter"))).collect(J.toJsonArray());
                Buffer value = filter.toBuffer();
                rocksDB.put(key.getBytes(),value.getBytes());
                p.complete();
            } catch (RocksDBException e) {
                logger.error("del client:{} subscription :{} failed",e,clientId,topics);
                throw new RuntimeException(e);
            }

        },promise);
        return promise.future();
    }
}
