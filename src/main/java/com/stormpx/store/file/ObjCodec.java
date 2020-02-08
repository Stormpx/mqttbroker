package com.stormpx.store.file;

import com.stormpx.kit.MqttCodecUtil;
import com.stormpx.mqtt.MqttProperty;
import com.stormpx.kit.J;
import io.netty.buffer.ByteBuf;
import io.netty.util.CharsetUtil;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class ObjCodec {

    public MessageObj decodeMessageObj(Buffer buffer){
        ByteBuf byteBuf = buffer.getByteBuf();
        int rnf = byteBuf.readInt();
        int idBytesLength = byteBuf.readUnsignedShort();
        ByteBuf idBytes = byteBuf.readBytes(idBytesLength);

        int topicBytesLength = byteBuf.readUnsignedShort();
        ByteBuf topicBytes = byteBuf.readBytes(topicBytesLength);
        byte qosAndRetain = byteBuf.readByte();
        byte expiryTimestampFlag = byteBuf.readByte();
        Long expiryTimestamp=null;
        if (expiryTimestampFlag!=0){
             expiryTimestamp= byteBuf.readLong();
        }

        int payloadLength = byteBuf.readInt();
        ByteBuf payload = byteBuf.readBytes(payloadLength);
        JsonArray jsonArray = new JsonArray();
        while (byteBuf.readableBytes()>0) {
            JsonObject properties = decodeAsProperties(byteBuf);
            jsonArray.add(properties);
        }
        JsonObject json = new JsonObject();
        json.put("id",idBytes.toString(StandardCharsets.UTF_8));
        json.put("topic",topicBytes.toString(StandardCharsets.UTF_8));
        json.put("qos",qosAndRetain>>1);
        boolean retain = (qosAndRetain&1)==1;
        json.put("retain", retain);
        if (expiryTimestampFlag!=0) {
            json.put("expiryTimestamp", expiryTimestamp);
        }
        json.put("payload",Buffer.buffer(payload).getBytes());
        json.put("properties",jsonArray);
        MessageObj messageObj = new MessageObj(json);
        messageObj.add(rnf);
        return messageObj;

    }

    public Buffer encodeMessageObj(MessageObj messageObj){
        int rnf = messageObj.getRnf();
        JsonObject jsonObject = messageObj.getMessage();
        String id = jsonObject.getString("id");
        String topic = jsonObject.getString("topic");
        Integer qos = jsonObject.getInteger("qos");
        Boolean retain = jsonObject.getBoolean("retain");
        Long expiryTimestamp = jsonObject.getLong("expiryTimestamp");
        byte[] payloads = jsonObject.getBinary("payload");
        JsonArray properties = jsonObject.getJsonArray("properties", J.EMPTY_ARRAY);

        byte[] idBytes = MqttCodecUtil.encodeUtf8String(id);

        byte[] topicBytes = MqttCodecUtil.encodeUtf8String(topic);

        byte qosARetain= encodeQosAndRetain(retain,qos);

        int size=4+2+idBytes.length+2+topicBytes.length+1+8+4+payloads.length;

        List<Buffer> buffers=new ArrayList<>();
        int propertiesSize=0;
        for (Object o : properties) {
            JsonObject json = (JsonObject) o;
            Buffer buffer = encodeAsProperties(json);
            propertiesSize+=buffer.length();
            buffers.add(buffer);
        }

        Buffer buffer = Buffer.buffer(size+propertiesSize);
        buffer.appendInt(rnf);
        buffer.appendUnsignedShort(idBytes.length)
                .appendBytes(idBytes);
        buffer.appendUnsignedShort(topicBytes.length)
                .appendBytes(topicBytes);
        buffer.appendByte(qosARetain);
        if (expiryTimestamp!=null){
            buffer.appendByte((byte) 1);
            buffer.appendLong(expiryTimestamp);
        }else {
            buffer.appendByte((byte) 0);
        }
        buffer.appendInt(payloads.length)
                .appendBytes(payloads);

        for (Buffer b : buffers) {
            buffer.appendBuffer(b);
        }
        return buffer;

    }

    public SessionObj decodeSessionObj(Buffer buffer){
        ByteBuf byteBuf = buffer.getByteBuf();
        int clientIdLength = byteBuf.readUnsignedShort();
        String clientId=slice(byteBuf,clientIdLength).toString(StandardCharsets.UTF_8);

        long expiryTimestamp = byteBuf.readLong();
        SessionObj sessionObj = new SessionObj(clientId);
        sessionObj.setExpiryTimestamp(expiryTimestamp);
        //will
        boolean isWill = byteBuf.readByte()!=0;
        if (isWill) {
            int topicLength = byteBuf.readUnsignedShort();
            ByteBuf topicBytes = byteBuf.readBytes(topicLength);

            byte qosAndRetain = byteBuf.readByte();
            boolean retain = (qosAndRetain & 1) == 1;
            int qos = qosAndRetain >> 1;
            long delayInterval = byteBuf.readLong();
            byte messageExpiryIntervalFlag = byteBuf.readByte();
            Long messageExpiryInterval=null;
            if (messageExpiryIntervalFlag!=0) {
                messageExpiryInterval = byteBuf.readUnsignedInt();
            }
            int payloadLength = byteBuf.readInt();
            ByteBuf payload = byteBuf.readBytes(payloadLength);
            //properties
            int propertiesSize=byteBuf.readInt();
            ByteBuf propertiesByteBuf = byteBuf.readBytes(propertiesSize);
            JsonArray properties = new JsonArray();
            while (propertiesByteBuf.readableBytes()>0) {
                properties.add(decodeAsProperties(propertiesByteBuf));
            }
            JsonObject will = new JsonObject();
            will.put("topic",topicBytes.toString(CharsetUtil.UTF_8));
            will.put("qos",qos);
            will.put("retain",retain);
            will.put("payload",Buffer.buffer(payload).getBytes());
            will.put("delayInterval",delayInterval);
            if (messageExpiryInterval!=null) {
                will.put("messageExpiryInterval", messageExpiryInterval);
            }
            will.put("properties",properties);
            sessionObj.setWill(will);
        }

        //topicSubscriptions
        int subscriptionSize=byteBuf.readInt();
        ByteBuf subscriptionByteBuf = slice(byteBuf,subscriptionSize);
        JsonArray subscription = new JsonArray();
        while (subscriptionByteBuf.readableBytes()>0){
            int topicFilterBytes = subscriptionByteBuf.readUnsignedShort();
            ByteBuf topicFilter = slice(subscriptionByteBuf,topicFilterBytes);
            byte qosAndNoLocalAndRetainAsPublished = subscriptionByteBuf.readByte();
            boolean noLocal=(qosAndNoLocalAndRetainAsPublished&1)==1;
            boolean retainAsPublished=(qosAndNoLocalAndRetainAsPublished&2)==2;
            JsonObject j = new JsonObject();
            j.put("topicFilter",topicFilter.toString(CharsetUtil.UTF_8));
            j.put("qos",qosAndNoLocalAndRetainAsPublished>>2);
            j.put("noLocal",noLocal);
            j.put("retainAsPublished",retainAsPublished);
            subscription.add(j);
        }
        sessionObj.addTopicSubscription(subscription);

        //messageLink

        int messageLinkSize=byteBuf.readInt();

        ByteBuf messageLinkByteBuf =slice(byteBuf,messageLinkSize);
        while (messageLinkByteBuf.readableBytes()>0){
            int linkLength = messageLinkByteBuf.readInt();
            ByteBuf messageLinkBuf = slice(messageLinkByteBuf,linkLength);
            JsonObject messageLink = decodeAsMessageLink(messageLinkBuf,true);
            sessionObj.addMessageLink(messageLink.getInteger("packetId"),messageLink);

        }


        //pending
        int pendingSize=byteBuf.readInt();
        ByteBuf pendingByteBuf = slice(byteBuf, pendingSize);
        while (pendingByteBuf.readableBytes()>0){
            int linkLength = pendingByteBuf.readInt();
            ByteBuf pendingBuf = slice(pendingByteBuf,linkLength);
            JsonObject messageLink = decodeAsMessageLink(pendingBuf,false);
            sessionObj.addPendingId(messageLink.getString("id"),messageLink);
        }

        //packetId
        while (byteBuf.readableBytes()>0){
            sessionObj.addPacketId(byteBuf.readUnsignedShort());
        }

        return sessionObj;
    }
    /**
     * encodeSessionObj
     * @param session
     * @return
     */
    public Buffer encodeSessionObj(SessionObj session){
        String clientId = session.getClientId();
        Long expiryTimestamp = session.getExpiryTimestamp();
        if (expiryTimestamp==null)
            expiryTimestamp=0L;
        JsonObject will = session.getWill();
        JsonArray topicSubscriptions = new JsonArray(session.getTopicSubscriptions().getList());
        Map<Integer, JsonObject> messageLinkMap = new LinkedHashMap<>(session.getMessageLinkMap());
        Map<String, JsonObject> pendingIds = new LinkedHashMap<>(session.getPendingMessage());
        Set<Integer> idSet = new LinkedHashSet<>(session.getPacketIdSet());

        byte[] clientIdBytes = MqttCodecUtil.encodeUtf8String(clientId);

        int size=2+clientIdBytes.length+9;
        //will
        Buffer willBuffer=null;
        if (will!=null) {
            String topic = will.getString("topic");
            Integer qos = will.getInteger("qos");
            Boolean retain = will.getBoolean("retain");
            Long delayInterval = will.getLong("delayInterval",0L);
            Long messageExpiryInterval = will.getLong("messageExpiryInterval");
            byte[] payloads = will.getBinary("payload");
            JsonArray properties = will.getJsonArray("properties", J.EMPTY_ARRAY);

            byte[] topicBytes = MqttCodecUtil.encodeUtf8String(topic);

            byte qosARetain = encodeQosAndRetain(retain, qos);

            size += (2 + topicBytes.length + 1 + 4+5 + 4 + payloads.length);

            List<Buffer> propertiesBuffers = new ArrayList<>();
            int propertiesSize = 0;
            for (Object o : properties) {
                JsonObject json = (JsonObject) o;
                Buffer buffer = encodeAsProperties(json);
                propertiesSize += buffer.length();
                propertiesBuffers.add(buffer);
            }
            size += 4 + propertiesSize;

            willBuffer=Buffer.buffer(4+propertiesSize);
            //will
            willBuffer.appendUnsignedShort(topicBytes.length)
                    .appendBytes(topicBytes);

            willBuffer.appendByte(qosARetain);
            willBuffer.appendLong(delayInterval);
            if (messageExpiryInterval!=null) {
                willBuffer.appendByte((byte) 1);
                willBuffer.appendUnsignedInt(messageExpiryInterval);
            }else{
                willBuffer.appendByte((byte) 0);
            }
            willBuffer.appendInt(payloads.length)
                    .appendBytes(payloads);

            //properties
            willBuffer.appendInt(propertiesSize);
            for (Buffer propertiesBuffer : propertiesBuffers) {
                willBuffer.appendBuffer(propertiesBuffer);
            }

        }
        //topicSubscriptions
        List<Buffer> subscriptionBuffers=new ArrayList<>();
        int subscriptionSize=0;
        for (Object o : topicSubscriptions) {
            JsonObject json = (JsonObject) o;
            String topicFilter = json.getString("topicFilter");
            byte[] topicFilterBytes = MqttCodecUtil.encodeUtf8String(topicFilter);
            byte qosAndNoLocalAndRetainAsPublished = encodeQosAndNoLocalAndRetainAsPublished(json.getInteger("qos"),
                    json.getBoolean("noLocal", false), json.getBoolean("retainAsPublished", false));

            Buffer buffer=Buffer.buffer(2+topicFilterBytes.length+1)
                .appendUnsignedShort(topicFilterBytes.length)
                .appendBytes(topicFilterBytes)
                .appendByte(qosAndNoLocalAndRetainAsPublished);

            subscriptionSize+=buffer.length();

            subscriptionBuffers.add(buffer);
        }
        size+=4+subscriptionSize;

        //messageLink
        List<Buffer> messageLinkBuffers=new ArrayList<>();
        int messageLinkSize=0;
        for (JsonObject json : messageLinkMap.values()) {
            synchronized(clientId+"link"+json.getInteger("packetId")) {
                json=json.copy();
            }
            Buffer buffer = encodeAsMessageLink(json.copy());
            messageLinkSize += 4 + buffer.length();
            messageLinkBuffers.add(buffer);
        }
        size+=4+messageLinkSize;

        //pending id
        List<Buffer> pendingIdBuffers=new ArrayList<>();
        int pendingIdSize=0;
        for (JsonObject json : pendingIds.values()) {
            Buffer buffer = encodeAsMessageLink(json);
            pendingIdSize+=4+buffer.length();
            pendingIdBuffers.add(buffer);
        }
        size+=4+pendingIdSize;

        //packetId
        size+=(idSet.size()*4);

        //write
        Buffer buffer = Buffer.buffer(size);
        buffer.appendUnsignedShort(clientIdBytes.length)
                .appendBytes(clientIdBytes);

        buffer.appendLong(expiryTimestamp);
        if (willBuffer!=null) {
            buffer.appendByte((byte) 1);
            buffer.appendBuffer(willBuffer);
        }
        else{
            buffer.appendByte((byte) 0);
        }

        //topicSubscriptions
        buffer.appendInt(subscriptionSize);
        for (Buffer subscriptionBuffer : subscriptionBuffers) {
            buffer.appendBuffer(subscriptionBuffer);
        }
        //messageLink
        buffer.appendInt(messageLinkSize);
        for (Buffer messageLinkBuffer : messageLinkBuffers) {
            buffer.appendInt(messageLinkBuffer.length());
            buffer.appendBuffer(messageLinkBuffer);
        }
        //pending
        buffer.appendInt(pendingIdSize);
        for (Buffer pendingIdBuffer : pendingIdBuffers) {
            buffer.appendInt(pendingIdBuffer.length());
            buffer.appendBuffer(pendingIdBuffer);
        }

        //packetId
        for (Integer id : idSet) {
            buffer.appendUnsignedShort(id);
        }
        return buffer;
    }


    private Buffer encodeAsProperties(JsonObject json){
        Integer property = json.getInteger("property");
        MqttProperty mqttProperty = MqttProperty.valueOf(property.byteValue());
        Buffer buffer = null;
        if (mqttProperty.isBinaryData()){
            byte[] values = json.getBinary("value");
            buffer=Buffer.buffer(3+values.length)
                    .appendByte(mqttProperty.byteValue())
                    .appendUnsignedShort(values.length)
                    .appendBytes(values);
        }else if (mqttProperty.isStringPair()){
            String key = json.getString("key");
            String v = json.getString("value");
            byte[] keyBytes = MqttCodecUtil.encodeUtf8String(key);
            byte[] valueBytes = MqttCodecUtil.encodeUtf8String(v);
            buffer = Buffer.buffer(3+ keyBytes.length + 2 + valueBytes.length);
            buffer.appendByte(mqttProperty.byteValue());
            buffer.appendUnsignedShort(keyBytes.length)
                    .appendBytes(keyBytes);
            buffer.appendUnsignedShort(valueBytes.length)
                    .appendBytes(valueBytes);
        }else if (mqttProperty.isByte()){
            Integer i = json.getInteger("value");
            buffer=Buffer.buffer(2).appendByte(mqttProperty.byteValue()).appendByte(i.byteValue());
        }else if (mqttProperty.isString()){
            byte[] bytes = MqttCodecUtil.encodeUtf8String(json.getString("value"));
            buffer=Buffer.buffer(3+bytes.length).appendByte(mqttProperty.byteValue()).appendUnsignedShort(bytes.length).appendBytes(bytes);
        }else if (mqttProperty.isFourByteInteger()){
            buffer=Buffer.buffer(5).appendByte(mqttProperty.byteValue()).appendUnsignedInt(json.getLong("value"));
        }else if (mqttProperty.isTwoByteInteger()){
            buffer=Buffer.buffer(3).appendByte(mqttProperty.byteValue()).appendUnsignedShort(json.getInteger("value"));
        }else if (mqttProperty.isVariableByteInteger()){
            buffer=Buffer.buffer(5).appendByte(mqttProperty.byteValue()).appendInt(json.getInteger("value"));
        }
        return buffer;
    }

    private JsonObject decodeAsProperties(ByteBuf buf){
        byte b = buf.readByte();
        JsonObject json = new JsonObject();
        json.put("property",b);
        MqttProperty mqttProperty = MqttProperty.valueOf(b);
        if (mqttProperty.isBinaryData()){
            int dataLength = buf.readUnsignedShort();
            json.put("value",Buffer.buffer(buf.readBytes(dataLength)).getBytes());
        }else if (mqttProperty.isStringPair()){
            int keyLength = buf.readUnsignedShort();
            ByteBuf key = buf.readBytes(keyLength);
            int valueLength = buf.readUnsignedShort();
            ByteBuf value = buf.readBytes(valueLength);
            json.put("key",key.toString(CharsetUtil.UTF_8));
            json.put("value",value.toString(CharsetUtil.UTF_8));
        }else if (mqttProperty.isByte()){
            byte aByte = buf.readByte();
            json.put("value",aByte);
        }else if (mqttProperty.isString()){
            int length = buf.readUnsignedShort();
            ByteBuf value = buf.readBytes(length);
            json.put("value",value.toString(CharsetUtil.UTF_8));
        }else if (mqttProperty.isFourByteInteger()){
            json.put("value",buf.readUnsignedInt());
        }else if (mqttProperty.isTwoByteInteger()){
            json.put("value",buf.readUnsignedShort());
        }else if (mqttProperty.isVariableByteInteger()){
            json.put("value",buf.readInt());
        }
        return json;
    }

    private JsonObject decodeAsMessageLink(ByteBuf buf,boolean decodePacketId){
        int idLength = buf.readInt();
        ByteBuf idBytes = buf.readBytes(idLength);
        int clientIdLength = buf.readUnsignedShort();
        ByteBuf clientIdByteBuf = buf.readBytes(clientIdLength);
        Integer packetId=null;
        if (decodePacketId) {
            packetId = buf.readUnsignedShort();
        }
        byte qosAndRetain = buf.readByte();
        int qos=qosAndRetain>>1;
        boolean retain=(qosAndRetain&1)==1;

        JsonArray jsonArray = new JsonArray();
        while (buf.readableBytes()>0){
            jsonArray.add(buf.readInt());
        }

        JsonObject messageLink = new JsonObject();
        messageLink.put("id",idBytes.toString(CharsetUtil.UTF_8));
        messageLink.put("clientId",clientIdByteBuf.toString(CharsetUtil.UTF_8));
        if (decodePacketId) {
            messageLink.put("packetId", packetId);
        }
        messageLink.put("qos",qos);
        messageLink.put("retain",retain);
        messageLink.put("subscriptionId",jsonArray);
        return messageLink;
    }

    private Buffer encodeAsMessageLink(JsonObject json){
        String id = json.getString("id");
        String clientId = json.getString("clientId");
        Integer packetId = json.getInteger("packetId");
        Boolean retain = json.getBoolean("retain",false);
        Integer qos = json.getInteger("qos",0);
        JsonArray subscriptionId = json.getJsonArray("subscriptionId",J.EMPTY_ARRAY);
        byte[] idBytes = MqttCodecUtil.encodeUtf8String(id);
        byte[] clientIdBytes = MqttCodecUtil.encodeUtf8String(clientId);
        byte qosAndRetain = encodeQosAndRetain(retain, qos);

        int size=4+idBytes.length+2+clientIdBytes.length+1+(subscriptionId.size()*4);
        if (packetId!=null)
            size+=2;

        Buffer buffer = Buffer.buffer(size);

        buffer.appendInt(idBytes.length)
                .appendBytes(idBytes);
        buffer.appendUnsignedShort(clientIdBytes.length)
            .appendBytes(clientIdBytes);
        if (packetId!=null)
            buffer.appendUnsignedShort(packetId);

        buffer.appendByte(qosAndRetain);

        for (Object o : subscriptionId) {
            Integer sid= (Integer) o;
            buffer.appendInt(sid);
        }
        return buffer;
    }

    private byte encodeQosAndRetain(boolean retain,int qos){
        byte qosARetain= (byte) (retain?1:0);
        qosARetain|=qos<<1;
        return qosARetain;
    }

    private byte encodeQosAndNoLocalAndRetainAsPublished(int qos,boolean noLocal,boolean retainAsPublished){
        byte b=0;
        if (noLocal) {
            b|=1;
        }
        if (retainAsPublished) {
            b|=2;
        }
        b|=qos<<2;
        return b;
    }


    private ByteBuf slice(ByteBuf buf,int length){
        ByteBuf slice = buf.slice(buf.readerIndex(), length);
        buf.skipBytes(length);
        return slice;
    }

}
