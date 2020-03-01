package com.stormpx.cluster;

import com.stormpx.kit.TopicFilter;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.MessageCodec;

import java.util.Collection;
import java.util.Set;

public class TopicMatchResult {
    public final static TopicMatchResultCodec CODEC=new TopicMatchResultCodec();

    private Collection<TopicFilter.SubscribeInfo> subscribeInfos;
    private Set<String> allNodeIds;

    public Collection<TopicFilter.SubscribeInfo> getSubscribeInfos() {
        return subscribeInfos;
    }

    public TopicMatchResult setSubscribeInfos(Collection<TopicFilter.SubscribeInfo> subscribeInfos) {
        this.subscribeInfos = subscribeInfos;
        return this;
    }

    public Set<String> getAllNodeIds() {
        return allNodeIds;
    }

    public TopicMatchResult setAllNodeIds(Set<String> allNodeIds) {
        this.allNodeIds = allNodeIds;
        return this;
    }

    private static class TopicMatchResultCodec implements MessageCodec<TopicMatchResult,TopicMatchResult> {


        @Override
        public void encodeToWire(Buffer buffer, TopicMatchResult topicMatchResult) {

        }

        @Override
        public TopicMatchResult decodeFromWire(int pos, Buffer buffer) {
            return null;
        }

        @Override
        public TopicMatchResult transform(TopicMatchResult topicMatchResult) {
            return topicMatchResult;
        }

        @Override
        public String name() {
            return "topicMatchResult";
        }

        @Override
        public byte systemCodecID() {
            return -1;
        }
    }
}
