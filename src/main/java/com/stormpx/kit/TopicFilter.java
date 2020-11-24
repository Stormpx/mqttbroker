package com.stormpx.kit;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.stream.Stream;

public class TopicFilter {


    private Map<String, TopicGroup> topics;

    public TopicFilter() {
        topics=new HashMap<>();
    }


    private TopicGroup getTopic(String topic){
        TopicGroup topicGroup = topics.get(topic);
        if (topicGroup ==null){
            topicGroup =new TopicGroup();
            topics.put(topic, topicGroup);
        }
        return topicGroup;
    }


    public boolean subscribe(String topic, String clientId, MqttQoS mqttQoS,boolean noLocal,boolean retainAsPublish,int identifier){
        if (topic==null||topic.length()==0)
            return false;

        boolean b = anySubscribed(topic);

        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){
                Entry entry = new Entry(topic, clientId, false, retainAsPublish, mqttQoS, identifier, true);
                getTopic(split[2]).getOrCreateShareGroup(split[1]).subscribe(entry);
                return b;
            }
        }


        Entry entry = new Entry(topic, clientId, noLocal, retainAsPublish, mqttQoS, identifier, false);
        getTopic(topic).getGroup().subscribe(entry);
        return b;
    }


    public boolean subscribed(String topic, String clientId){
        if (topic==null||topic.length()==0)
            return false;

        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){
                return getTopic(split[2]).getShareGroup(split[1]).subscribed(clientId);
            }
        }
        return getTopic(topic).getGroup().subscribed(clientId);
    }


    public boolean unSubscribe(String topic, String clientId){
        if (topic==null||topic.length()==0)
            return false;
        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){

                SubscribeGroup shareGroup = getTopic(split[2]).getShareGroup(split[1]);
                if (shareGroup!=null)
                    shareGroup.unSubscribe(clientId);

                return anySubscribed(topic);
            }
        }
        getTopic(topic).getGroup().unSubscribe(clientId);

        return anySubscribed(topic);
    }

    public Stream<String> clearSubscribe(String clientId){
        if (clientId==null)return Stream.empty();
        return topics.values()
                .stream()
                .map(so->{
                    Entry entry = so.getGroup().unSubscribe(clientId);
                    Stream<String> shareStream= so.shareGroups.values().stream().map(g -> g.unSubscribe(clientId)).filter(Objects::nonNull).map(Entry::getTopicFilterName);
                    return Stream.concat(Optional.ofNullable(entry).map(Entry::getTopicFilterName).stream(),shareStream);
                })
                .reduce(Stream::concat)
                .orElseGet(Stream::empty)
        ;
    }



    public boolean anySubscribed(String topic){
        if (topic.startsWith("$share/")){
            String[] split = topic.split("/", 3);
            if (!split[1].equals("#")&&!split[1].equals("+")&&split.length==3){

                SubscribeGroup shareGroup = getTopic(split[2]).getShareGroup(split[1]);
                return shareGroup!=null&&!shareGroup.isEmpty();
            }
        }
        return !getTopic(topic).getGroup().isEmpty();
    }

    public boolean anyMatch(String topic){
        return topics.entrySet().stream().anyMatch(t->TopicUtil.matches(t.getKey(),topic)&&t.getValue().anySubscribed());
    }

    public Collection<MatchResult> matches(String topic){
        BiConsumer<Map<String, MatchResult>, Entry> consumer=(map, e)->{
            var subscribeInfo = map.computeIfAbsent(e.getClientId(), (k) -> new MatchResult(e.getClientId()));
            subscribeInfo.allMatchSubscribe.add(e);
        };

        return topics.entrySet()
                .stream()
                .filter(e->TopicUtil.matches(e.getKey(),topic))
                .map(Map.Entry::getValue)
                .reduce(new HashMap<String, MatchResult>(),(map, sObj)->{
                    sObj.subscribeList().forEach(e-> consumer.accept(map,e));
                    sObj.shareSubscribeList().forEach(e-> consumer.accept(map,e));
                    return map;
                },(s1,s2)->s1)
                .values();

    }


    private class TopicGroup {

        private Map<String,SubscribeGroup> shareGroups;
        private SubscribeGroup group;

        public TopicGroup() {
            this.shareGroups =new HashMap<>();
            this.group=new SubscribeGroup();
        }

        public SubscribeGroup getGroup(){
            return group;
        }

        public SubscribeGroup getShareGroup(String shareName){
            return shareGroups.get(shareName);
        }

        public SubscribeGroup getOrCreateShareGroup(String shareName){
            return shareGroups.computeIfAbsent(shareName, (k) -> new SubscribeGroup());
        }

        public Collection<SubscribeGroup> shareGroups(){
            return shareGroups.values();
        }

        public Collection<Entry> subscribeList(){
            return group.getAll();
        }
        public Stream<Entry> shareSubscribeList(){
            return shareGroups
                    .values()
                    .stream()
                    .map(SubscribeGroup::chooseOne)
                    .filter(Optional::isPresent)
                    .map(Optional::get);

        }

        public boolean anySubscribed(){
            return !group.isEmpty()||shareGroups().stream().anyMatch(g -> !g.isEmpty());
        }

    }


    private class SubscribeGroup{
        private LinkedHashMap<String, Entry> map;

        private int index=0;

        public SubscribeGroup() {
            this.map=new LinkedHashMap<>();
        }

        public void subscribe(Entry entry){
            map.put(entry.clientId,entry);
        }

        public Entry unSubscribe(String clientId){
            Entry entry = map.remove(clientId);
            return entry;
        }

        public Optional<Entry> chooseOne(){
            if (index>=map.size())
                index=0;

            return map.values().stream().skip(index++).findFirst();
        }

        public Collection<Entry> getAll(){
            return map.values();
        }

        public boolean subscribed(String clientId){
            return map.containsKey(clientId);
        }

        public boolean isEmpty(){
            return map.isEmpty();
        }

    }


    public class MatchResult {
        private String clientId;
        private List<Entry> allMatchSubscribe=new LinkedList<>();

         MatchResult(String clientId) {
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
