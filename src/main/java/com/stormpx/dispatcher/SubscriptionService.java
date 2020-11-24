package com.stormpx.dispatcher;

import com.stormpx.dispatcher.command.SubscriptionsCommand;
import com.stormpx.dispatcher.command.UnSubscriptionsCommand;

public interface SubscriptionService {


    void subscribe(SubscriptionsCommand command);


    void unSubscribe(UnSubscriptionsCommand command);


}
