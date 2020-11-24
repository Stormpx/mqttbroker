package com.stormpx.store.rocksdb;

import com.stormpx.dispatcher.ClientSession;
import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.kit.Codec;
import com.stormpx.kit.value.Values1;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.store.BlockStore;
import com.stormpx.store.MessageLink;
import com.stormpx.store.SessionStore;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.*;

import java.util.*;

public class RocksDBSessionStore extends BlockStore implements SessionStore {
    private final static Logger logger= LoggerFactory.getLogger(RocksDBSessionStore.class);
    private ColumnFamilyHandle SESSION_COLUMN_FAMILY;
    private Vertx vertx;
    private RocksDB rocksDB;

    public RocksDBSessionStore(Vertx vertx) {
        super(vertx);
        this.vertx=vertx;
        this.rocksDB=Db.db();
        this.SESSION_COLUMN_FAMILY=Db.sessionColumnFamily();
    }

    private byte[] expiryTimestampKey(String clientId){
        return (clientId+"-expiryTimestamp").getBytes();
    }

    private byte[] offlineLinkKey(String clientId, String id){
        return (clientId+"-link-"+id).getBytes();
    }
    private byte[] linkKey(String clientId, Integer packetId){
        return (clientId + "-link-" + packetId).getBytes();
    }
    private String linkKeyPrefix(String clientId){
        return (clientId+"-link");
    }



    private byte[] packetIdKey(String clientId,Integer packetId){
        return (clientId+"-packetId-"+packetId).getBytes();
    }

    private String packetIdKeyPrefix(String clientId){
        return clientId+"-packetId-";
    }

    private byte[] willKey(String clientId){
        return (clientId+"-will").getBytes();
    }

    private byte[] subscribeKey(String clientId,String topic){
        String key=clientId+"-subscribe-"+topic;
        return key.getBytes();
    }
    private String subscribeKeyPrefix(String clientId){
        return clientId+"-subscribe-";
    }


    @Override
    public Future<ClientSession> get(String clientId) {
        return getExpiryTimestamp(clientId)
            .map(Values1::values)
            .compose(v->getWill(clientId).map(v::toValues2))
            .compose(v->getSubscription(clientId).map(v::toValues3))
            .compose(v->links(clientId).map(v::toValues4))
            .compose(v->packetId(clientId).map(v::toValues5))

            .map(v->{
                var session=new ClientSession(clientId);

                session.setExpiryTimestamp(v.getOne())
                        .setWill(v.getTwo())
                        .setTopicSubscriptions(v.getThree())
                        .setMessageLinks(v.getFour());

                List<Integer> ids = v.getFive();
                BitSet bitSet = new BitSet();
                ids.forEach(bitSet::set);
                session.setPacketIdSet(bitSet);
                return session;
            })
            .onFailure(t->logger.error("",t))    ;
    }

    @Override
    public Future<Void> save(ClientSession clientSession) {
        String clientId = clientSession.getClientId();

        writeOps(()->{
            WriteBatch batch = new WriteBatch();

            RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);
            for (rocksIterator.seek(clientId.getBytes());
                 rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(clientId);
                 rocksIterator.next()){
                batch.remove(SESSION_COLUMN_FAMILY,rocksIterator.key());
                rocksIterator.next();
            }
            rocksIterator.close();


            if (clientSession.getExpiryTimestamp()!=null) {
                byte[] value = Buffer.buffer().appendLong(clientSession.getExpiryTimestamp()).getBytes();
                batch.put(SESSION_COLUMN_FAMILY, expiryTimestampKey(clientId), value);
            }

            if (clientSession.getWill()!=null) {
                DispatcherMessage will = clientSession.getWill();
                byte[] key = willKey(clientId);
                batch.put(SESSION_COLUMN_FAMILY, key, Codec.encode(will));
            }

            if (clientSession.getTopicSubscriptions()!=null) {
                List<MqttSubscription> topicSubscriptions = clientSession.getTopicSubscriptions();
                for (MqttSubscription mqttSubscription : topicSubscriptions) {
                    byte[] key = subscribeKey(clientId,mqttSubscription.getTopicFilter());
                    batch.put(SESSION_COLUMN_FAMILY,key, Codec.encode(mqttSubscription));
                }
            }

            if (clientSession.getMessageLinks()!=null) {
                List<MessageLink> links = clientSession.getMessageLinks();
                for (MessageLink link : links) {
                    if (link.isOffLink()){
                        String id = link.getId();
                        batch.put(SESSION_COLUMN_FAMILY,offlineLinkKey(clientId, id),Codec.encode(link));
                    }else{
                        Integer packetId =link.getPacketId();
                        batch.put(SESSION_COLUMN_FAMILY,linkKey(clientId,packetId), Codec.encode(link));
                    }
                }
            }

            if (clientSession.getPacketIdSet()!=null){
                clientSession.getPacketIdSet().stream().forEach(packetId->{
                    try {
                        rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),packetIdKey(clientId,packetId),Buffer.buffer().appendInt(packetId).getBytes());
                    } catch (RocksDBException e) {
                        throw new RuntimeException(e);
                    }
                });
            }

            rocksDB.write(Db.asyncWriteOptions(),batch);
            return null;
        });
        return null;
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
        return writeOps(()->{
            delClient(clientId);
            return (Void)null;
        }).onFailure(e->logger.error("del session {} fail",e,clientId));

    }

    @Override
    public Future<Void> setExpiryTimestamp(String clientId, Long expiryTimestamp) {
        return writeOps(()->{
            byte[] value = Buffer.buffer().appendLong(expiryTimestamp).getBytes();
            rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),expiryTimestampKey(clientId),value);
            return (Void)null;
        }).onFailure(e->logger.error("set client: {} expiryTimestamp :{} failed",e,clientId,expiryTimestamp));
    }

    @Override
    public Future<Long> getExpiryTimestamp(String clientId) {

        return readOps(()->{
            byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,expiryTimestampKey(clientId));
            return value==null?null:Buffer.buffer(value).getLong(0);
        }).onFailure(e-> logger.error("get client:{} expiryTimestamp failed",e,clientId));
    }



    @Override
    public Future<Void> addLink(String clientId, MessageLink link) {
        return writeOps(()->{
            WriteBatch writeBatch = new WriteBatch();
            String id = link.getId();
            writeBatch.remove(SESSION_COLUMN_FAMILY, offlineLinkKey(clientId,id));
            Integer packetId =link.getPacketId();
            writeBatch.put(SESSION_COLUMN_FAMILY,linkKey(clientId,packetId), Codec.encode(link));
            rocksDB.write(Db.asyncWriteOptions(),writeBatch);
            return null;
        });
    }

    @Override
    public Future<Void> addOfflineLink(String clientId,MessageLink link){
        return writeOps(()->{
            WriteBatch writeBatch = new WriteBatch();
            writeBatch.put(SESSION_COLUMN_FAMILY,offlineLinkKey(clientId,link.getId()), Codec.encode(link));
            rocksDB.write(Db.asyncWriteOptions(),writeBatch);
            return null;
        });
    }

    @Override
    public Future<List<MessageLink>> links(String clientId) {
        return readOps(()->{
            String prefix=linkKeyPrefix(clientId);
            RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);
            rocksIterator.seek(prefix.getBytes());
            List<MessageLink> list=new ArrayList<>();
            for (rocksIterator.seek(prefix.getBytes());rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(prefix);rocksIterator.next()){
                byte[] value = rocksIterator.value();
                list.add(Codec.decodeMessageLink(value));
                rocksIterator.next();
            }
            rocksIterator.close();
            return list;
        });
    }
    @Override
    public Future<String> release(String clientId, int packetId) {
        return writeOps(()->{
            byte[] key=linkKey(clientId,packetId);
            byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key);
            if (value==null){
                return null;
            }
            rocksDB.delete(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key);
            MessageLink link = Codec.decodeMessageLink(value);
            return link.getId();
        });
    }

    @Override
    public Future<String> receive(String clientId, int packetId) {
        return writeOps(()->{
            byte[] key = linkKey(clientId,packetId);
            byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key);
            MessageLink messageLink = Codec.decodeMessageLink(value);
            rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key, Codec.encode(MessageLink.create(null,clientId,packetId,false, MqttQoS.AT_MOST_ONCE,null)));
            return messageLink.getId();
        });
    }



    @Override
    public Future<Void> addPacketId(String clientId, int packetId) {
        return writeOps(()->{
            rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),packetIdKey(clientId,packetId),Buffer.buffer().appendInt(packetId).getBytes());
            return (Void)null;
        });
    }

    @Override
    public Future<List<Integer>> packetId(String clientId) {
        return readOps(()->{
            String prefix=packetIdKeyPrefix(clientId);
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
            return list;
        });

    }

    @Override
    public Future<Void> removePacketId(String clientId, int packetId) {
        return writeOps(()->{
            rocksDB.delete(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),packetIdKey(clientId,packetId));
            return null;
        });
    }


    @Override
    public Future<Void> saveWill(String clientId, DispatcherMessage will) {
        return writeOps(()->{
            try {
                byte[] key = willKey(clientId);
                rocksDB.put(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),key, Codec.encode(will));
                return null;
            } catch (RocksDBException e) {
                logger.error("del client:{} will :{} failed",e,clientId,will);
                throw e;
            }
        });
    }

    @Override
    public Future<DispatcherMessage> getWill(String clientId) {
        return readOps(()->{
            try {
                byte[] key = willKey(clientId);
                byte[] value = rocksDB.get(SESSION_COLUMN_FAMILY,key);
                return value==null?null: Codec.decodeDispatcherMessage(Buffer.buffer(value));
            } catch (RocksDBException e) {
                logger.error("get client:{} will failed",e,clientId);
                throw  e;
            }
        });
    }

    @Override
    public Future<Void> delWill(String clientId) {
        return writeOps(()->{
            try {
                rocksDB.delete(SESSION_COLUMN_FAMILY,Db.asyncWriteOptions(),willKey(clientId));
                return null;
            } catch (RocksDBException e) {
                logger.error("del client:{} will failed",e,clientId);
                throw e;
            }
        });
    }


    @Override
    public Future<Void> addSubscription(String clientId, List<MqttSubscription> mqttSubscriptions) {

        return writeOps(()->{
            try {
                WriteBatch batch = new WriteBatch();
                for (MqttSubscription mqttSubscription : mqttSubscriptions) {
                    byte[] key = subscribeKey(clientId,mqttSubscription.getTopicFilter());
                    batch.put(SESSION_COLUMN_FAMILY,key, Codec.encode(mqttSubscription));
                }
                rocksDB.write(Db.asyncWriteOptions(),batch);
                return null;
            } catch (RocksDBException e) {
                logger.error("add client:{} subscription :{} failed",e,clientId,mqttSubscriptions);
                throw e;
            }
        });
    }

    @Override
    public Future<List<MqttSubscription>> getSubscription(String clientId) {
        return readOps(()->{
            String prefix = subscribeKeyPrefix(clientId);
            RocksIterator rocksIterator = rocksDB.newIterator(SESSION_COLUMN_FAMILY);

            List<MqttSubscription> list=new ArrayList<>();
            for (rocksIterator.seek(prefix.getBytes());
                 rocksIterator.isValid()&&new String(rocksIterator.key()).startsWith(prefix);
                 rocksIterator.next()) {
                byte[] value = rocksIterator.value();
                list.add(Codec.decodeMqttSubscription(value));
                rocksIterator.next();
            }
            rocksIterator.close();
            return list;
        });
    }

    @Override
    public Future<Void> deleteSubscription(String clientId, List<String> topics) {
        return writeOps(()->{
            try {
                WriteBatch batch = new WriteBatch();
                for (String topic : topics) {
                    byte[] key = subscribeKey(clientId,topic);
                    batch.delete(SESSION_COLUMN_FAMILY,key);
                }
                rocksDB.write(Db.asyncWriteOptions(),batch);
                return null;
            } catch (RocksDBException e) {
                logger.error("del client:{} subscription :{} failed",e,clientId,topics);
                throw e;
            }
        });


    }
}
