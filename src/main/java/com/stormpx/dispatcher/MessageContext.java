package com.stormpx.dispatcher;

import java.time.Instant;
import java.util.*;

public class MessageContext {
    //publish clientId
    private String clientId;

    private Integer packetId;

    private List<Integer> subscriptionIds=new ArrayList<>();

    private Set<String> shareTopics=new HashSet<>();

    private boolean isFromCluster;

    private DispatcherMessage message;



    public MessageContext(DispatcherMessage message) {
        this.message = message;
    }





    /**
     * 设置消息超时时间
     * @param expiryInterval mqtt消息里带的时间
     * @param maxExpiryInterval 配置里设置的最大时间
     * @return
     */
    public MessageContext setExpiryTimeStamp(Long expiryInterval,Long maxExpiryInterval){
        if (expiryInterval==null||(maxExpiryInterval!=null&&maxExpiryInterval<expiryInterval)){
            expiryInterval=maxExpiryInterval;
        }
        if (expiryInterval!=null){
            message.setMessageExpiryTimestamp(Instant.now().getEpochSecond() + expiryInterval) ;
        }
        return this;
    }




    public MessageContext setClientId(String clientId) {
        this.clientId = clientId;
        return this;
    }



    public String getId() {
        return message.getId();
    }

    public String getClientId() {
        return clientId;
    }


    public DispatcherMessage getMessage() {
        return message;
    }

    public Set<String> getShareTopics() {
        return shareTopics;
    }

    public MessageContext setShareTopics(Set<String> shareTopics) {
        this.shareTopics = shareTopics;
        return this;
    }


    public Integer getPacketId() {
        return packetId;
    }

    public MessageContext setPacketId(Integer packetId) {
        this.packetId = packetId;
        return this;
    }

    public MessageContext setSubscriptionIds(List<Integer> subscriptionIds) {
        this.subscriptionIds = subscriptionIds;
        return this;
    }

    public List<Integer> getSubscriptionIds() {
        return subscriptionIds;
    }

    public boolean isFromCluster() {
        return isFromCluster;
    }

    public MessageContext setFromCluster(boolean fromCluster) {
        isFromCluster = fromCluster;
        return this;
    }
}

