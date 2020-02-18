package com.stormpx.store.file;

import com.stormpx.kit.MqttCodecUtil;
import com.stormpx.kit.TopicUtil;
import com.stormpx.store.TimeoutWill;
import com.stormpx.kit.J;
import com.stormpx.Constants;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.vertx.core.*;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

public class MqttStoreVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger(MqttStoreVerticle.class);


    private JsonObject storeConfig;

    //key topic value messageId
    private Map<String,String> retainMessageMap;
    //key Id value message
    private PublishMessageStore publishMessageStore;

    private SessionStore sessionStore;
    //dispatcherTime
    private Queue<TimeoutWill> timeoutWillQueue;

    private TimeoutStream timeoutStream;

    private TimeoutStream expiryStream;

    protected void onConfigRefresh(JsonObject config) {
        this.storeConfig=Optional.ofNullable(config).orElseGet(JsonObject::new);
    }

    @Override
    public void start(Promise<Void> startPromise) throws Exception {
        this.publishMessageStore=new PublishMessageStore();
        this.retainMessageMap =new ConcurrentHashMap<>();
        this.sessionStore=new SessionStore();
        this.timeoutWillQueue= new PriorityQueue<>(Comparator.comparingLong(TimeoutWill::getDispatcherTime));


        onConfigRefresh(config());

        tryLoadData().compose(v->{
            vertx.eventBus().consumer("_mqtt_store_")
                    .handler(message->{
                        String action = message.headers().get("action");
                        if (action==null)
                            return;
                        Object body = message.body();
                        logger.debug("receive request action:{} body:{}",deploymentID(),action,body);
                        switch (action){
                            case "clearSession":
                                clearSession((JsonObject) body);
                                break;
                            case "storeMessage":
                                storeMessage((JsonObject)body);
                                break;
                            case "storeSessionState":
                                storeSessionState((JsonObject) body);
                                break;
                            case "fetchSessionState":
                                message.reply(fetchSessionState((JsonObject)body));
                                break;
                            case "fetchUnReleaseMessage":
                                message.reply(fetchUnReleaseMessage((JsonObject)body));
                                break;
                            case "link":
                                link((JsonObject)body);
                                break;
                            case "release":
                                release((JsonObject)body);
                                break;
                            case "receive":
                                receive((JsonObject)body);
                                break;
                            case "storeSubscription":
                                storeSubscription((JsonObject) body);
                                break;
                            case "fetchSubscription":
                                message.reply(fetchSubscription((JsonObject)body));
                                break;
                            case "deleteSubscription":
                                deleteSubscription((JsonObject)body);
                                break;
                            case "addPacketId":
                                addPacketId((JsonObject)body);
                                break;
                            case "unacknowledgedPacketId":
                                message.reply(unacknowledgedPacketId((JsonObject)body));
                                break;
                            case "removePacketId":
                                removePacketId((JsonObject)body);
                                break;
                            case "storeWillMessage":
                                storeWillMessage((JsonObject)body);
                                break;
                            case "fetchWillMessage":
                                message.reply(fetchWillMessage((JsonObject)body));
                                break;
                            case "dropWillMessage":
                                dropWillMessage((JsonObject)body);
                                break;
                            case "filterMatchMessage":
                                message.reply(filterMatchMessage((JsonObject)body));
                                break;
                            case "addTimeoutWill":
                                addTimeoutWill((JsonObject)body);
                                break;
                            case "deleteTimeoutWill":
                                deleteTimeoutWill((JsonObject)body);
                                break;
                            case "fetchFirstTimeoutWill":
                                message.reply(fetchFirstTimeoutWill());
                                break;
                        }
                    });



            if (storeConfig.getBoolean(Constants.SAVE_ENABLE,false)){
                // configurable
                Integer interval = storeConfig.getInteger(Constants.SAVE_INTERVAL);
                if (interval==null)
                    interval=60;
                this.timeoutStream=vertx.periodicStream(1000*interval);
                timeoutStream.handler(id->{
                    String storeConfigString = storeConfig.getString(Constants.SAVE_DIR);
                    if (storeConfigString != null && !storeConfigString.isBlank()) {
                        long currentTimeMillis = System.currentTimeMillis();
                        writeFile(Path.of(storeConfigString).normalize().toString())
                            .setHandler(ar->{
                                if (ar.failed())
                                    logger.error("save data fail",ar.cause());
                                else
                                    logger.debug("save data success cost:{}ms",System.currentTimeMillis()-currentTimeMillis);
                            });
                    }
                });
            }
            this.expiryStream=vertx.periodicStream(60*1000).handler(id->{
                logger.debug("start clean expiry message");
                long epochSecond = Instant.now().getEpochSecond();
                publishMessageStore.keys().forEach(s->{
                    MessageObj messageObj = publishMessageStore.getObj(s);
                    if (messageObj==null)
                        return;
                    if (messageObj.getRnf()<=0&&epochSecond-messageObj.getTimestamp()>60){
                        publishMessageStore.remove(s);
                    }else {
                        Long expiryTimestamp = messageObj.getMessage().getLong("expiryTimestamp");
                        if (expiryTimestamp != null && epochSecond > expiryTimestamp) {
                            publishMessageStore.remove(s);
                        }
                    }
                });
            });

            return Future.<Void>succeededFuture();

        }).setHandler(startPromise);


    }


    private Future<Void> tryLoadData(){
        if (!storeConfig.getBoolean(Constants.SAVE_ENABLE,false))
            return Future.succeededFuture();
        String folder = storeConfig.getString(Constants.SAVE_DIR);
        if (folder==null||folder.isBlank()){
            return Future.failedFuture("save folder is null");
        }


        Promise<Void> promise=Promise.promise();
        vertx.executeBlocking(p->{
            if (!vertx.fileSystem().existsBlocking(folder))
                vertx.fileSystem().mkdirBlocking(folder);

            List<String> list = vertx.fileSystem().readDirBlocking(folder);
            for (String s : list) {

                if (!new File(s).getName().equals("data.db"))
                    continue;

                Buffer buffer = vertx.fileSystem().readFileBlocking(s);
                if (buffer.getLong(0)!=30)
                    throw new RuntimeException("unrecognized file head");

                ObjCodec objCodec = new ObjCodec();
                ByteBuf buf = buffer.getByteBuf().skipBytes(8);
                while (buf.isReadable()){
                    byte b = buf.readByte();
                    if (b==1){
                        int topicLength = buf.readUnsignedShort();
                        ByteBuf topic = buf.readBytes(topicLength);
                        int idLength = buf.readInt();
                        ByteBuf id = buf.readBytes(idLength);
                        retainMessageMap.put(topic.toString(CharsetUtil.UTF_8),id.toString(CharsetUtil.UTF_8));
                    }else if (b==2){
                        int length = buf.readInt();
                        ByteBuf byteBuf = buf.readBytes(length);
                        MessageObj messageObj = objCodec.decodeMessageObj(Buffer.buffer(byteBuf));
                        if (logger.isDebugEnabled())
                            logger.debug("verticle:{} read messageObj from db.da:{}",deploymentID(),messageObj.getMessage());
                        publishMessageStore.put(messageObj.getMessage().getString("id"),messageObj);
                    }else if (b==3){
                        int length = buf.readInt();
                        ByteBuf byteBuf = buf.readBytes(length);
                        SessionObj sessionObj = objCodec.decodeSessionObj(Buffer.buffer(byteBuf));
                        if (logger.isDebugEnabled())
                            logger.debug("verticle:{} read sessionObj from db.da:{}",deploymentID(),sessionObj);
                        sessionStore.putSession(sessionObj.getClientId(),sessionObj);
                    }else
                        throw new RuntimeException("unrecognized file content");

                }

                break;
            }
            p.complete();
        },promise);


        return promise.future();
    }



    /**
     * 持久化操作
     * @param path
     */
    private Future<Void> writeFile(String path){

        Promise<Void> p=Promise.promise();
        vertx.executeBlocking(promise->{
            String randomName = UUID.randomUUID().toString();
            String randomPath = path + "/" + randomName + ".da";
            AsyncFile asyncFile = vertx.fileSystem().openBlocking(randomPath, new OpenOptions().setCreateNew(true).setWrite(true));
            asyncFile.write(Buffer.buffer().appendLong(30));

            for (String topic : retainMessageMap.keySet()) {
                String id = retainMessageMap.get(topic);
                if (id==null)
                    continue;
                if (logger.isDebugEnabled())
                    logger.debug("verticle:{} ready write retain message topic:{} id:{} ",deploymentID(),topic,id);
                byte[] topicBytes = MqttCodecUtil.encodeUtf8String(topic);
                byte[] idBytes = MqttCodecUtil.encodeUtf8String(id);
                Buffer buffer = Buffer.buffer(1+ 2 + topicBytes.length + 4 + idBytes.length);
                buffer.appendByte((byte) 1);
                buffer.appendUnsignedShort(topicBytes.length);
                buffer.appendBytes(topicBytes);
                buffer.appendInt(idBytes.length);
                buffer.appendBytes(idBytes);
                asyncFile.write(buffer);
            }
            asyncFile.flush();

            ObjCodec objCodec = new ObjCodec();

            for (String messageId : publishMessageStore.keys()) {
                MessageObj messageObj = publishMessageStore.getObj(messageId);
                if (messageObj==null)
                    continue;
                Long expiryTimestamp = messageObj.getMessage().getLong("expiryTimestamp");
                if (expiryTimestamp!=null&& Instant.now().getEpochSecond()>expiryTimestamp) {
                    continue;
                }
                if (logger.isDebugEnabled())
                    logger.debug("verticle:{} ready encode messageObj :{}",deploymentID(),messageObj.getMessage());
                Buffer buffer = objCodec.encodeMessageObj(messageObj);
                asyncFile.write(Buffer.buffer().appendByte((byte) 2));
                asyncFile.write(Buffer.buffer().appendInt(buffer.length()));
                asyncFile.write(buffer);
            }
            asyncFile.flush();


            for (String clientId : sessionStore.keys()) {
                SessionObj sessionObj = sessionStore.getSession(clientId);
                if (sessionObj==null)
                    continue;
                if (logger.isDebugEnabled())
                    logger.debug("verticle:{} ready encode sessionObj :{}",deploymentID(),sessionObj);
                Buffer buffer = objCodec.encodeSessionObj(sessionObj);
                asyncFile.write(Buffer.buffer().appendByte((byte) 3));
                asyncFile.write(Buffer.buffer().appendInt(buffer.length()));
                asyncFile.write(buffer);
            }

            asyncFile.end(ar->{
                if (ar.succeeded()){
                    vertx.fileSystem().move(randomPath,path+"/data.db",new CopyOptions().setAtomicMove(true).setReplaceExisting(true),promise);
                }else{
                    promise.fail(ar.cause());
                }
            });


        },true,p);

        return p.future();
    }


    private void clearSession(JsonObject body) {
        String clientId = body.getString("clientId");
        SessionObj session = sessionStore.getSession(clientId);
        if (session!=null){
            sessionStore.deleteSession(clientId);
            Stream<String> stream = session.getPendingMessage().keySet().stream();
            Stream<String> stream0 = session.getMessageLinkMap()
                    .values()
                    .stream()
                    .filter(link -> link.getString("id") != null)
                    .map(link -> link.getString("id"));
            Stream.concat(stream,stream0)
                    .forEach(id->{
                        MessageObj obj = publishMessageStore.getObj(id);
                        if (obj!=null)
                            obj.add(-1);
                    });

        }

    }


    private void storeMessage(JsonObject body){
        String id = body.getString("id");
        String topic = body.getString("topic");
        Boolean retain = body.getBoolean("retain", false);
        // payload length=0 delete all message
        MessageObj messageObj = publishMessageStore.getObj(id);
        if (messageObj!=null)
            return;

        publishMessageStore.put(id,body);
        if (retain) {
            byte[] payload = body.getBinary("payload");
            if (payload.length == 0) {
                String oldId = retainMessageMap.remove(topic);
                if (oldId!=null)
                    publishMessageStore.add(oldId,-1);
                return;
            }
            retainMessageMap.put(topic,id);
            publishMessageStore.add(id,1);
        }
    }

    private void storeSessionState(JsonObject body) {
        String clientId = body.getString("clientId");
        Long expiryTimestamp = body.getLong("expiryTimestamp");

        sessionStore.setExpiryTimestamp(clientId,expiryTimestamp);

    }

    private JsonObject fetchSessionState(JsonObject body) {
        String clientId = body.getString("clientId");

        Long expiryTimestamp = sessionStore.getExpiryTimestamp(clientId);
        if (expiryTimestamp==null)
            return null;

        return new JsonObject()
                .put("clientId",clientId)
                .put("expiryTimestamp",expiryTimestamp);

    }

    private JsonArray fetchUnReleaseMessage(JsonObject body) {
        String clientId = body.getString("clientId");

        Stream<JsonObject> pendingSteam = sessionStore.getPendingId(clientId).map(json -> {
            String id = json.getString("id");
            if (id == null) return json;
            JsonObject message = publishMessageStore.get(id);
            if (message == null) {
                return null;
            }
            json = message.copy().mergeIn(json);
            return json;
        }).filter(Objects::nonNull);
        Stream<JsonObject> unReleaseStream = sessionStore.getMessageLink(clientId).map(json -> {
            String id = json.getString("id");
            if (id == null) return json;
            JsonObject message = publishMessageStore.get(id);
            if (message == null) {
                sessionStore.release(clientId, json.getInteger("packetId"));
                return null;
            }
            json = message.copy().mergeIn(json);
            return json;
        }).filter(Objects::nonNull);

        return Stream.concat(pendingSteam,unReleaseStream).collect(J.toJsonArray());

    }


    /**
     * add message link
     * @param body
     */
    private void link(JsonObject body) {
        String clientId = body.getString("clientId");
        String id = body.getString("id");
        sessionStore.addMessageLink(clientId,body);
        publishMessageStore.add(id,1);
    }

    /**
     * remove message link
     * @param body
     */
    private void release(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        String id = sessionStore.release(clientId, packetId);
        //ackPacket id != null
        if (id!=null)
            publishMessageStore.add(id,-1);
    }

    /**
     * remove message link.id
     * @param body
     */
    private void receive(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        String id = sessionStore.receive(clientId, packetId);
        if (id!=null)
            publishMessageStore.add(id,-1);
    }

    private void storeSubscription(JsonObject body) {
        String clientId = body.getString("clientId");
        JsonArray subscriptions = body.getJsonArray("subscriptions",J.EMPTY_ARRAY);
        sessionStore.addTopicSubscription(clientId,subscriptions);
    }

    private JsonArray fetchSubscription(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.getTopicSubscriptions(clientId);
    }

    private void deleteSubscription(JsonObject body) {
        String clientId = body.getString("clientId");
        JsonArray topics = body.getJsonArray("topics",J.EMPTY_ARRAY);
        sessionStore.deleteSubscription(clientId,topics);
    }

    private void addPacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        sessionStore.addPacketId(clientId,packetId);
    }

    private JsonArray unacknowledgedPacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        return new JsonArray(sessionStore.getPacketIdSet(clientId));
    }
    private void removePacketId(JsonObject body) {
        String clientId = body.getString("clientId");
        Integer packetId = body.getInteger("packetId");
        sessionStore.removePacketId(clientId,packetId);
    }


    private void storeWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        JsonObject will = body.getJsonObject("will");
        sessionStore.setWill(clientId,will);
    }
    private JsonObject fetchWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        return sessionStore.getWill(clientId);
    }
    private void dropWillMessage(JsonObject body) {
        String clientId = body.getString("clientId");
        sessionStore.setWill(clientId,null);
    }

    private JsonArray filterMatchMessage(JsonObject body) {
        JsonArray topicFilters = body.getJsonArray("topicFilters");
        Map<String, String> retainMap = this.retainMessageMap;

        return retainMap.keySet()
                .stream()
                .filter(topic->topicFilters.stream().anyMatch(o-> TopicUtil.matches(o.toString(),topic)))
                .map(retainMap::get)
                .filter(Objects::nonNull)
                .map(publishMessageStore::get)
                .filter(Objects::nonNull)
                .collect(J.toJsonArray());

    }

    private void addTimeoutWill(JsonObject body) {
        timeoutWillQueue.add(TimeoutWill.fromJson(body));
    }

    private void deleteTimeoutWill(JsonObject body) {
        String clientId = body.getString("clientId");
        timeoutWillQueue.removeIf(timeoutWill -> timeoutWill.getClientId().equals(clientId));
    }

    private JsonObject fetchFirstTimeoutWill() {
        if (timeoutWillQueue.isEmpty())
            return null;
        return timeoutWillQueue.peek().toJson();
    }


    public void stop(Promise<Void> stopPromise) throws Exception {
        if (timeoutStream!=null)
            timeoutStream.cancel();
        if (expiryStream!=null)
            expiryStream.cancel();
        stopPromise.complete();
    }

}
