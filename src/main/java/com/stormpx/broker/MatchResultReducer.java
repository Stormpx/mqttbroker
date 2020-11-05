package com.stormpx.broker;

import com.stormpx.kit.TopicFilter;

import java.util.*;
import java.util.stream.Collectors;

public class MatchResultReducer {

    private List<TopicFilter.Entry> matchResult;

    private TopicFilter.Entry maxQosEntry;

    private Set<TopicFilter.Entry> shareTopic;

    private List<Integer> subscriptionIds;

    public MatchResultReducer(List<TopicFilter.Entry> matchResult) {
        this.matchResult = matchResult;
    }

    public MatchResultReducer reduce(Set<String> allowedShareTopic){

        this.shareTopic =new HashSet<>();
        this.subscriptionIds=new LinkedList<>();

        this.maxQosEntry=matchResult.stream()
                .peek(e->{
                    if (e.isShare()&&allowedShareTopic.contains(e.getTopicFilterName())){
                        this.shareTopic.add(e);
                    }
                    if (e.getSubscriptionIdentifier()!=0)
                        this.subscriptionIds.add(e.getSubscriptionIdentifier());
                })
                .max(Comparator.comparingInt(e->e.getMqttQoS().value()))
                .orElse(maxQosEntry);

        return this;
    }

    public TopicFilter.Entry getMaxQosEntry() {
        return maxQosEntry;
    }

    public Set<TopicFilter.Entry> getShareTopic() {
        return shareTopic;
    }

    public Set<String> getShareTopicName() {
        return shareTopic.stream().map(TopicFilter.Entry::getTopicFilterName).collect(Collectors.toSet());
    }

    public List<Integer> getSubscriptionIds() {
        return subscriptionIds;
    }
}
