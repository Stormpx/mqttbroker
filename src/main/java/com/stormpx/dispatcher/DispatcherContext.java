package com.stormpx.dispatcher;

import com.stormpx.kit.TopicFilter;
import io.vertx.core.Vertx;

public class DispatcherContext {

    private Vertx vertx;

    private TopicFilter topicFilter;

    private MessageService messageService;

    private SessionService sessionService;

    private boolean cluster;

    public DispatcherContext(Vertx vertx, TopicFilter topicFilter, MessageService messageService, SessionService sessionService,boolean cluster) {
        this.vertx = vertx;
        this.topicFilter = topicFilter;
        this.messageService = messageService;
        this.sessionService = sessionService;
        this.cluster=cluster;
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

    public boolean isCluster() {
        return cluster;
    }

}
