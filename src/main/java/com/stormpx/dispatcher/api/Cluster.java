package com.stormpx.dispatcher.api;

import com.stormpx.cluster.mqtt.ActionLog;
import com.stormpx.cluster.mqtt.RetainMatchResult;
import com.stormpx.cluster.mqtt.SessionResult;
import com.stormpx.cluster.mqtt.TopicMatchResult;
import com.stormpx.cluster.mqtt.command.RequestMessageCommand;
import com.stormpx.cluster.mqtt.command.SendMessageCommand;
import com.stormpx.dispatcher.DispatcherMessage;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;

import java.util.List;

public class Cluster extends DK {
    public final static String LOG_PROPOSAL="_log_proposal_";

    public final static String SESSION_REQUEST="_session_request_";
    public final static String RETAIN_MESSAGE_MATCH="_retain_message_match_";
    public final static String MESSAGE_REQUEST="_message_request_";
    public final static String TOPIC_MATCH="_topic_match_";
    public final static String SEND_MESSAGE="_send_message_";

    private Vertx vertx;
    private String id;

    public Cluster(Vertx vertx,String id) {
        super(vertx);
        this.vertx = vertx;
        this.id=id;
    }



    public void proposal(ActionLog actionLog){
        vertx.eventBus().send(LOG_PROPOSAL,actionLog);
    }


   public Future<SessionResult> requestSession(String clientId){
        return request(SESSION_REQUEST,clientId);
   }

   public Future<RetainMatchResult> retainMatch(List<String> topicFilters){

       return request(RETAIN_MESSAGE_MATCH,new JsonArray(topicFilters));
   }

   public Future<DispatcherMessage> requestMessage(RequestMessageCommand command){
       return request(MESSAGE_REQUEST,command);
   }


   public Future<TopicMatchResult> topicMatch(String topic){
       return request(TOPIC_MATCH,topic);
   }

   public void sendMessage(SendMessageCommand command){
        vertx.eventBus().send(SEND_MESSAGE, command);
   }

    public String getId() {
        return id;
    }




}
