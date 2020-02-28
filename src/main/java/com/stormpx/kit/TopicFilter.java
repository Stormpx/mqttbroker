package com.stormpx.kit;

import com.stormpx.ex.ProtocolErrorException;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class TopicFilter {


    private Map<String, SubscribeObj> topics;

    public TopicFilter() {
        topics=new HashMap<>();
    }


    private SubscribeObj getObj(String topic){
        SubscribeObj subscribeObj = topics.get(topic);
        if (subscribeObj==null){
            subscribeObj=new SubscribeObj();
            topics.put(topic,subscribeObj);
        }
        return subscribeObj;
    }


    public boolean subscribe(String topic, String clientId, MqttQoS mqttQoS,boolean noLocal,boolean retainAsPublish,int identifier){
        if (topic==null||topic.length()==0)
            return false;
        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){
                /*if (noLocal)
                    throw new ProtocolErrorException("it is a Protocol Error to set the No Local bit to 1 on a Shared Subscription");
*/
                getObj(split[2]).addShare(topic,split[1],clientId,mqttQoS, false,retainAsPublish,identifier);
                return true;
            }
        }

        getObj(topic).addNonShare(topic,clientId,mqttQoS,noLocal,retainAsPublish, identifier);
        return true;
    }


    public boolean subscribed(String topic, String clientId){
        if (topic==null||topic.length()==0)
            return false;

        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){
                return getObj(split[2]).shareSubscribed(clientId,split[1]);
            }
        }
        return getObj(topic).nonShareSubscribed(clientId);
    }

    public boolean unSubscribe(String topic, String clientId){
        if (topic==null||topic.length()==0)
            return false;
        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){
                getObj(split[2]).removeShare(split[1],clientId);
                return true;
            }
        }
        getObj(topic).removeNonShare(clientId);
        return true;
    }

    public void clearSubscribe(String clientId){
        if (clientId==null)return;
        topics.values().forEach(so->{
            so.removeNonShare(clientId);
            so.removeShare(clientId);
        });
    }


  /*  public Collection<Entry> matches(String topic){
        List<Entry> list=new ArrayList<>();
        Collection<Entry> collection=topics.entrySet()
                .stream()
                .filter(e->TopicUtil.matches(e.getKey(),topic))
                .map(Map.Entry::getValue)
                .peek(subscribeObj -> list.addAll(subscribeObj.shareSubscribeList()))
                .map(SubscribeObj::subscribeList)
                .reduce(new HashMap<Entry, Entry>(),(map, s2)->{
                    s2.forEach(subscribeEntry ->
                            map.compute(subscribeEntry,
                                    (k, v)-> v!=null&&v.getMqttQoS().value()> subscribeEntry.getMqttQoS().value()?v: subscribeEntry));
                    return map;
                },(s1,s2)->s1)
                .values();

        list.addAll(collection);

        return list;
    }*/


    public boolean anySubscribed(String topicFilter){
        SubscribeObj subscribeObj = topics.get(topicFilter);
        if (subscribeObj==null)
            return false;

        return subscribeObj.anySubscribed();
    }

    public boolean anyMatch(String topic){
        return topics.entrySet().stream().anyMatch(t->TopicUtil.matches(t.getKey(),topic)&&t.getValue().anySubscribed());
    }

    public Collection<SubscribeInfo> matches(String topic){
        BiConsumer<Map<String,SubscribeInfo>, Entry> consumer=(map, e)->{
            var subscribeInfo = map.computeIfAbsent(e.getClientId(), (k) -> new SubscribeInfo(e.getClientId()));
            subscribeInfo.allMatchSubscribe.add(e);
        };

        return topics.entrySet()
                .stream()
                .filter(e->TopicUtil.matches(e.getKey(),topic))
                .map(Map.Entry::getValue)
                .reduce(new HashMap<String, SubscribeInfo>(),(map, sObj)->{
                    sObj.subscribeList().forEach(e-> consumer.accept(map,e));
                    sObj.shareSubscribeList().forEach(e-> consumer.accept(map,e));
                    return map;
                },(s1,s2)->s1)
                .values();

    }



    public Entry findQos(String clientId, String topic){
        return topics.entrySet()
                .stream()
                .filter(e->e.getKey().matches(topic))
                .map(Map.Entry::getValue)
                .filter(subscribeObj -> subscribeObj.nonShareSubscribed(clientId))
                .findAny()
                .map(subscribeObj -> subscribeObj.fetchNonShareInfo(clientId))
                .get();

    }




    private class SubscribeObj{

        Map<String,Map<String, Entry>> shareSubscribeMap;
        Map<String, Entry> nonShareSubscribeMap;

        public SubscribeObj() {
            shareSubscribeMap=new HashMap<>();
            nonShareSubscribeMap =new HashMap<>();
        }


        public void addNonShare(String topicName,String clientId, MqttQoS qos,boolean noLocal,boolean retainAsPublished,int subscriptionIdentifier){
            nonShareSubscribeMap.put(clientId, new Entry(topicName,clientId,noLocal,retainAsPublished,qos,subscriptionIdentifier,false));
        }
        public void addShare(String topicName,String shareName,String clientId, MqttQoS qos,boolean noLocal,boolean retainAsPublished,int subscriptionIdentifier){
            Map<String, Entry> subscribeInfoMap = shareSubscribeMap.computeIfAbsent(shareName, (k) -> new HashMap<>());
            subscribeInfoMap.put(clientId,new Entry(topicName,clientId,noLocal,retainAsPublished,qos,subscriptionIdentifier,true));
        }

        public void removeNonShare(String clientId){
            nonShareSubscribeMap.remove(clientId);
        }
        public void removeShare(String clientId){
            shareSubscribeMap.values().forEach(m->{
                    m.remove(clientId);
            });
        }
        public void removeShare(String shareName,String clientId){
            Map<String, Entry> map = shareSubscribeMap.get(shareName);
            if (map!=null)
                map.remove(clientId);
        }
        public boolean nonShareSubscribed(String clientId){
            return nonShareSubscribeMap.containsKey(clientId);
        }
        public boolean shareSubscribed(String clientId,String shareName){
            Map<String, Entry> subscribeInfoMap = shareSubscribeMap.get(shareName);
            return subscribeInfoMap!=null&&subscribeInfoMap.containsKey(clientId);
        }

        public Entry fetchNonShareInfo(String clientId){
            return nonShareSubscribeMap.get(clientId);
        }
        public Entry fetchShareInfo(String clientId, String shareName){
            Map<String, Entry> subscribeInfoMap = shareSubscribeMap.get(shareName);
            if (subscribeInfoMap==null)
                return null;
            return subscribeInfoMap.get(clientId);
        }

        public Collection<Entry> subscribeList(){
            return nonShareSubscribeMap.values();
        }
        public Collection<Entry> shareSubscribeList(){
            return shareSubscribeMap.values()
                    .stream()
                    .map(map->map.values().stream().unordered().findAny())
                    .filter(Optional::isPresent)
                    .map(Optional::get)
                    .collect(Collectors.toList());
        }

        public boolean anySubscribed(){
            return !nonShareSubscribeMap.isEmpty()||shareSubscribeMap.values().stream().anyMatch(map -> !map.isEmpty());
        }

    }


    public class SubscribeInfo{
        private String clientId;
        private List<Entry> allMatchSubscribe=new LinkedList<>();

         SubscribeInfo(String clientId) {
            this.clientId = clientId;

        }

        public String getClientId() {
            return clientId;
        }


        public List<Entry> getAllMatchSubscribe() {
            return allMatchSubscribe;
        }

        @Override
        public String toString() {
            return "SubscribeInfo{" + "clientId='" + clientId + '\'' + ", allMatchSubscribe=" + allMatchSubscribe + '}';
        }
    }

    public class Entry {
        private String topicFilterName;
        private String clientId;
        private boolean noLocal;
        private boolean retainAsPublished;
        private MqttQoS mqttQoS;
        private int subscriptionIdentifier;
        private boolean share;

         Entry(String topic, String clientId, boolean noLocal, boolean retainAsPublished, MqttQoS mqttQoS, int subscriptionIdentifier, boolean share) {
             this.topicFilterName=topic;
            this.clientId = clientId;
            this.noLocal = noLocal;
            this.retainAsPublished = retainAsPublished;
            this.mqttQoS = mqttQoS;
            this.subscriptionIdentifier = subscriptionIdentifier;
            this.share=share;
        }

         Entry(String clientId, MqttQoS mqttQoS) {
            this.clientId = clientId;
            this.mqttQoS = mqttQoS;
        }

        public String getClientId() {
            return clientId;
        }


        public MqttQoS getMqttQoS() {
            return mqttQoS;
        }

        public boolean isNoLocal() {
            return noLocal;
        }

        public boolean isRetainAsPublished() {
            return retainAsPublished;
        }
        public int getSubscriptionIdentifier() {
            return subscriptionIdentifier;
        }

        public boolean isShare() {
            return share;
        }
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Entry that = (Entry) o;
            return Objects.equals(clientId, that.clientId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(clientId);
        }


        public String getTopicFilterName() {
            return topicFilterName;
        }

        @Override
        public String toString() {
            return "Entry{" + "topicFilterName='" + topicFilterName + '\'' + ", clientId='" + clientId + '\'' + ", noLocal=" + noLocal + ", retainAsPublished=" + retainAsPublished + ", mqttQoS=" + mqttQoS + ", subscriptionIdentifier=" + subscriptionIdentifier + ", share=" + share + '}';
        }
    }
}
