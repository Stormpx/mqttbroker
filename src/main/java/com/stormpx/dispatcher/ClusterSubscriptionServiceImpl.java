package com.stormpx.dispatcher;

import com.stormpx.dispatcher.api.Cluster;
import com.stormpx.cluster.mqtt.ActionLog;

import java.util.List;

public class ClusterSubscriptionServiceImpl extends SubscriptionServiceImpl {


    public ClusterSubscriptionServiceImpl(DispatcherContext dispatcherContext, Cluster cluster) {
        super(dispatcherContext);

        setSubscriptionHook(new SubscriptionHook() {
            @Override
            public void onTopicSubscribe(List<String> topics) {
                cluster.proposal(ActionLog.subscribe(cluster.getId(),topics));
            }

            @Override
            public void onTopicUnSubscribe(List<String> topics) {
                cluster.proposal(ActionLog.unSubscribe(cluster.getId(),topics));
            }
        });
    }
}
