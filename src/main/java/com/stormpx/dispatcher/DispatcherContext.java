package com.stormpx.dispatcher;

import com.stormpx.kit.TopicFilter;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

public class DispatcherContext {


    private Vertx vertx;

    private JsonObject config;

    private TopicFilter topicFilter;

    private MessageService messageService;

    private SessionService sessionService;

    private SubscriptionService subscriptionService;

    public DispatcherContext(Vertx vertx, TopicFilter topicFilter) {
        this.vertx = vertx;
        this.topicFilter = topicFilter;
    }

    public Vertx getVertx() {
        return vertx;
    }

    public TopicFilter getTopicFilter() {
        return topicFilter;
    }

    public MessageService getMessageService() {
        return messageService;
    }

    public SessionService getSessionService() {
        return sessionService;
    }


    public SubscriptionService getSubscriptionService() {
        return subscriptionService;
    }

    public DispatcherContext setMessageService(MessageService messageService) {
        this.messageService = messageService;
        return this;
    }

    public DispatcherContext setSessionService(SessionService sessionService) {
        this.sessionService = sessionService;
        return this;
    }

    public DispatcherContext setSubscriptionService(SubscriptionService subscriptionService) {
        this.subscriptionService = subscriptionService;
        return this;
    }

    public JsonObject getConfig() {
        return config;
    }

    public DispatcherContext setConfig(JsonObject config) {
        this.config = config;
        return this;
    }
}
