package com.stormpx.dispatcher;

import java.util.List;

public interface SubscriptionHook {

    void onTopicSubscribe(List<String> topics);

    void onTopicUnSubscribe(List<String> topics);


}
