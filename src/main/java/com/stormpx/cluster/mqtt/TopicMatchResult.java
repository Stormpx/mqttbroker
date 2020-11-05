package com.stormpx.cluster.mqtt;

import com.stormpx.kit.TopicFilter;

import java.util.Collection;
import java.util.Set;

public class TopicMatchResult {

    private Collection<TopicFilter.MatchResult> matchResults;
    private Set<String> allNodeIds;

    public Collection<TopicFilter.MatchResult> getMatchResults() {
        return matchResults;
    }

    public TopicMatchResult setMatchResults(Collection<TopicFilter.MatchResult> matchResults) {
        this.matchResults = matchResults;
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
