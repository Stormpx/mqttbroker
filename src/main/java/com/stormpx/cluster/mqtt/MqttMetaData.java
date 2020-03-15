package com.stormpx.cluster.mqtt;

import com.stormpx.cluster.snapshot.SnapshotWriter;
import com.stormpx.kit.MsgPackUtil;
import com.stormpx.kit.TopicFilter;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;

import java.nio.charset.StandardCharsets;
import java.util.*;

public class MqttMetaData {

    private TopicFilter topicFilter;
    //key nodeId value topic
    private Map<String, Set<String>> subscribeMap;
    //key nodeId value requestId
    private Map<String,Set<Integer>> idempotentMap;
    //key topic value id
    private Map<String,String> retainMap;
    //key id value nodeIds
    private Map<String, Set<String>> idIndexMap;
    //key clientId value nodeIds
    private Map<String,Set<String>> sessionMap;

    public MqttMetaData() {
        this.topicFilter=new TopicFilter();
        this.subscribeMap=new HashMap<>();
        this.retainMap=new HashMap<>();
        this.idIndexMap=new HashMap<>();
        this.sessionMap=new HashMap<>();
        this.idempotentMap=new HashMap<>();
    }

    public void addSubscription(String nodeId, List<String> topics){
        Set<String> set = subscribeMap.computeIfAbsent(nodeId, (k) -> new HashSet<>());
        for (String topic : topics) {
            set.add(topic);
            this.topicFilter.subscribe(topic,nodeId, MqttQoS.EXACTLY_ONCE,false,false,0);
        }
    }

    public void delSubscription(String nodeId, List<String> topics){
        Set<String> set = subscribeMap.get(nodeId);
        for (String topic : topics) {
            if (set!=null)
                set.remove(topic);
            this.topicFilter.unSubscribe(topic,nodeId);
        }
    }

    public void saveMessage(String nodeId,String id){
        Set<String> nodeSet = idIndexMap.computeIfAbsent(id, (k) -> new HashSet<>());
        nodeSet.add(nodeId);
    }

    public void delMessage(String nodeId,String id){
        Set<String> set = idIndexMap.get(id);
        if (set!=null)
            set.remove(nodeId);
    }

    public void putRetain(String topic,String id){
        retainMap.put(topic,id);
    }

    public void removeRetain(String topic){
        retainMap.remove(topic);
    }

    public void clearSession(String clientId){
        sessionMap.remove(clientId);
    }

    public void saveSession(String nodeId,String clientId){
        Set<String> nodeSet = sessionMap.computeIfAbsent(clientId, k -> new HashSet<>());
        nodeSet.add(nodeId);
    }

    public void removeSession(String nodeId,String clientId){
        Set<String> set = sessionMap.get(clientId);
        if (set!=null)
            set.remove(nodeId);
    }


    public void setExecute(String nodeId,int proposalId){
        Set<Integer> set = idempotentMap.computeIfAbsent(nodeId, k -> new TreeSet<>());
        set.add(proposalId);
    }

    public void discardProposalId(String nodeId,int proposalId){
        Set<Integer> set = idempotentMap.get(nodeId);
        if (set!=null){
            TreeSet<Integer> treeSet = (TreeSet) set;
            Integer id;
            while ((id=treeSet.floor(proposalId))!=null){
                treeSet.remove(id);
            }
        }
    }

    public boolean isExecuted(String nodeId,int proposalId){
        Set<Integer> set = idempotentMap.get(nodeId);
        return set!=null&&set.contains(proposalId);
    }

    public Set<String> getSessionIndex(String clientId){
        return sessionMap.get(clientId);
    }

    public MqttMetaData copy(){
        MqttMetaData mqttMetaData = new MqttMetaData();
        mqttMetaData.subscribeMap=Map.copyOf(subscribeMap);
        mqttMetaData.idempotentMap=Map.copyOf(idempotentMap);
        mqttMetaData.retainMap=Map.copyOf(retainMap);
        mqttMetaData.idIndexMap=Map.copyOf(idIndexMap);
        mqttMetaData.sessionMap=Map.copyOf(sessionMap);
        return mqttMetaData;
    }

    public TopicFilter getTopicFilter() {
        return topicFilter;
    }

    public Map<String, String> getRetainMap() {
        return retainMap;
    }

    public Set<String> getMessageIndex(String id) {
        return idIndexMap.get(id);

    }






    public Buffer encodeSubscribe(){
        Set<Map.Entry<String, Set<String>>> subscribeEntrySet = subscribeMap.entrySet();


        List<Buffer> subscribeBuffers=new ArrayList<>(subscribeEntrySet.size());

        int subscribeBufferSize=encodeKeyArray(subscribeEntrySet,subscribeBuffers);

        Buffer subscribeBuffer = Buffer.buffer(subscribeBufferSize+1);
        //subscribe type
        subscribeBuffer.appendByte((byte) 0);
        subscribeBuffers.forEach(subscribeBuffer::appendBuffer);
        return subscribeBuffer;
    }

    public Buffer encodeRequestId(){
        Set<Map.Entry<String, Set<Integer>>> requestEntries = idempotentMap.entrySet();
        int size=0;
        List<Buffer> buffers=new ArrayList<>();

        for (Map.Entry<String, Set<Integer>> entry : requestEntries) {
            String nodeId = entry.getKey();
            Set<Integer> set = entry.getValue();
            Buffer str = MsgPackUtil.encodeStr(nodeId);
            size+=str.length();
            buffers.add(str);
            Buffer arrayLength = MsgPackUtil.encodeArrayLength(set.size());
            size+=arrayLength.length();
            buffers.add(arrayLength);
            for (Integer integer : set) {
                Buffer buffer = MsgPackUtil.encodeInt(integer);
                size+=buffer.length();
                buffers.add(buffer);
            }
        }

        Buffer buffer = Buffer.buffer(size+1);
        //idempotent type
        buffer.appendByte((byte) 1);
        buffers.forEach(buffer::appendBuffer);
        return buffer;
    }

    public Buffer encodeRetain(){
        Set<Map.Entry<String, String>> retainEntries = retainMap.entrySet();
        int size=0;
        List<Buffer> buffers=new ArrayList<>();
        for (Map.Entry<String, String> entry : retainEntries) {
            String key = entry.getKey();
            String value = entry.getValue();
            Buffer topic = MsgPackUtil.encodeStr(key);
            size+=topic.length();
            Buffer id = MsgPackUtil.encodeStr(value);
            size+=id.length();

            buffers.add(topic);
            buffers.add(id);
        }

        Buffer buffer = Buffer.buffer(size+1);
        //retain type
        buffer.appendByte((byte) 2);
        buffers.forEach(buffer::appendBuffer);
        return buffer;
    }


    public Buffer encodeIdIndex() {
        Set<Map.Entry<String, Set<String>>> idIndexEntries = idIndexMap.entrySet();
        List<Buffer> buffers=new ArrayList<>(idIndexEntries.size());

        int size=encodeKeyArray(idIndexEntries,buffers);

        Buffer buffer = Buffer.buffer(size+1);
        //id type
        buffer.appendByte((byte) 3);
        buffers.forEach(buffer::appendBuffer);
        return buffer;
    }

    public Buffer encodeSessionIndex() {
        Set<Map.Entry<String, Set<String>>> sessionIndexEntries = sessionMap.entrySet();
        List<Buffer> buffers=new ArrayList<>(sessionIndexEntries.size());

        int size=encodeKeyArray(sessionIndexEntries,buffers);

        Buffer buffer = Buffer.buffer(size+1);
        //session type
        buffer.appendByte((byte) 4);
        buffers.forEach(buffer::appendBuffer);
        return buffer;
    }

    private int encodeKeyArray(Set<Map.Entry<String, Set<String>>> entries,List<Buffer> buffers){
        int size=0;

        for (Map.Entry<String, Set<String>> entry : entries) {
            String key = entry.getKey();
            Set<String> set = entry.getValue();
            Buffer str = MsgPackUtil.encodeStr(key);
            size+=str.length();
            buffers.add(str);
            Buffer arrayLength = MsgPackUtil.encodeArrayLength(set.size());
            size+=arrayLength.length();
            buffers.add(arrayLength);
            for (String string : set) {
                Buffer encodeStr = MsgPackUtil.encodeStr(string);
                size+=encodeStr.length();
                buffers.add(encodeStr);
            }
        }

        return size;
    }

    public void decode(Buffer buffer){
        MetaDataDecoder metaDataDecoder = new MetaDataDecoder();
        metaDataDecoder.decode(buffer);

    }

    @Override
    public String toString() {
        return "MqttMetaData{" + "subscribeMap=" + subscribeMap + ", idempotentMap=" + idempotentMap + ", retainMap=" + retainMap + ", idIndexMap=" + idIndexMap + ", sessionMap=" + sessionMap + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttMetaData that = (MqttMetaData) o;
        return Objects.equals(subscribeMap, that.subscribeMap) && Objects.equals(idempotentMap, that.idempotentMap) && Objects.equals(retainMap, that.retainMap) && Objects.equals(idIndexMap, that.idIndexMap) && Objects.equals(sessionMap, that.sessionMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(subscribeMap, idempotentMap, retainMap, idIndexMap, sessionMap);
    }

    public class MetaDataDecoder{


        private Byte currentType;

        private String currentString;
        private int arrayLength;

        private void onNumber(Number number){
            if (arrayLength!=0) {
                setExecute(currentString, number.intValue());
                arrayLength--;
            }
        }
        private void onString(String str){
            if (currentType==0){
                if (currentString==null){
                    this.currentString=str;
                }else{
                    addSubscription(currentString, Collections.singletonList(str));
                    arrayLength-=1;
                    if (arrayLength==0)
                        this.currentString=null;
                }
            }else if (currentType==1){
                this.currentString=str;
            } else if (currentType==2){
                if (currentString==null)
                    this.currentString=str;
                else {
                    putRetain(currentString, str);
                    this.currentString=null;
                }
            }else if (currentType==3){
                if (currentString==null){
                    this.currentString=str;
                }else{
                    saveMessage(str,currentString);
                    arrayLength-=1;
                    if (arrayLength==0)
                        this.currentString=null;
                }
            }else if (currentType==4){
                if (currentString==null){
                    this.currentString=str;
                }else{
                    saveSession(str,currentString);
                    arrayLength-=1;
                    if (arrayLength==0)
                        this.currentString=null;
                }
            }
        }

        public void decode(Buffer buffer){
            ByteBuf buf = buffer.getByteBuf();
            while (buf.isReadable()){
                byte b = buf.readByte();
                switch (b){
                    case 0:
                    case 1:
                    case 2:
                    case 3:
                    case 4:
                        currentType=b;
                        this.currentString=null;
                        break;
                    case (byte) 0xd0:
                        onNumber(buf.readByte());
                        break;
                    case (byte) 0xd1:
                        onNumber(buf.readShort());
                        break;
                    case (byte) 0xd2:
                        onNumber(buf.readInt());
                        break;
                    case (byte) 0xd3:
                        onNumber(buf.readLong());
                        break;
                    case (byte) 0xd9:
                        onString(buf.readBytes(buf.readUnsignedByte()).toString(StandardCharsets.UTF_8));;
                        break;
                    case (byte) 0xda:
                        onString(buf.readBytes(buf.readUnsignedShort()).toString(StandardCharsets.UTF_8));
                        break;
                    case (byte) 0xdb:
                        onString(buf.readBytes(buf.readInt()).toString(StandardCharsets.UTF_8));
                        break;
                    case (byte) 0xdc:
                        this.arrayLength = buf.readUnsignedShort();
                        break;
                    case (byte) 0xdd:
                        this.arrayLength = buf.readInt();
                        break;
                    default:
                        //101xxxxx
                        if ((b& 0xa0)==0xa0)
                            onString(buf.readBytes(b&0b00011111).toString(StandardCharsets.UTF_8));
                        else if ((b& 0x90)==0x90)
                        //1001xxxx
                            this.arrayLength=(b&0b00001111);
                }
            }


        }



    }


}
