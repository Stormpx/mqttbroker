package com.stormpx.cluster.mqtt;

import com.stormpx.kit.TopicFilter;

import java.util.Collection;
import java.util.Set;

public class TopicMatchResult {

    private Collection<TopicFilter.SubscribeMatchResult> subscribeMatchResults;
    private Set<String> allNodeIds;

    public Collection<TopicFilter.SubscribeMatchResult> getSubscribeMatchResults() {
        return subscribeMatchResults;
    }

    public TopicMatchResult setSubscribeMatchResults(Collection<TopicFilter.SubscribeMatchResult> subscribeMatchResults) {
        this.subscribeMatchResults = subscribeMatchResults;
        return this;
    }

    public Set<String> getAllNodeIds() {
        return allNodeIds;
    }

    public TopicMatchResult setAllNodeIds(Set<String> allNodeIds) {
        this.allNodeIds = allNodeIds;
        return this;
    }
}
