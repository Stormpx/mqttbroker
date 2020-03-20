package com.stormpx.cluster;

import com.stormpx.cluster.message.ActionLog;
import com.stormpx.cluster.mqtt.RetainMatchResult;
import com.stormpx.cluster.mqtt.SessionResult;
import com.stormpx.cluster.mqtt.TopicMatchResult;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.MessageObj;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Cluster {
    private final static String LOG_PROPOSAL="_log_proposal_";

    private final static String SESSION_REQUEST="_session_request_";
    private final static String RETAIN_MESSAGE_MATCH="_retain_message_match_";
    private final static String MESSAGE_REQUEST="_message_request_";
    private final static String MESSAGE_REQUEST_INDEX="_message_request_index_";
    private final static String TOPIC_MATCH="_topic_match_";
    private final static String SEND_MESSAGE="_send_message_";

    private Vertx vertx;

    public Cluster(Vertx vertx) {
        this.vertx = vertx;
    }

    public Consumer consumer(){
        return new Consumer(vertx);
    }




    public void proposal(ActionLog actionLog){
        vertx.eventBus().send(LOG_PROPOSAL,actionLog);
    }

   public Future<SessionResult> requestSession(String clientId){
       Promise<SessionResult> promise=Promise.promise();
        vertx.eventBus().<SessionResult>request(SESSION_REQUEST,clientId,ar->{
            if (ar.succeeded()){
                promise.complete(ar.result().body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
   }

   public Future<RetainMatchResult> retainMatch(List<String> topicFilters){
       Promise<RetainMatchResult> promise=Promise.promise();
       vertx.eventBus().<RetainMatchResult>request(RETAIN_MESSAGE_MATCH,new JsonArray(topicFilters), ar->{
           if (ar.succeeded()){
               promise.complete(ar.result().body());
           }else{
               promise.fail(ar.cause());
           }
       });
       return promise.future();
   }

   public Future<MessageObj> requestMessage(Set<String> nodeIds,String id){
       Promise<MessageObj> promise=Promise.promise();
       vertx.eventBus().<MessageObj>request(MESSAGE_REQUEST,new JsonObject().put("nodeIds",new ArrayList<>(nodeIds)).put("id",id),ar->{
          if (ar.succeeded()){
              promise.complete(ar.result().body());
          }else{
              promise.fail(ar.cause());
          }
       });
       return promise.future();
   }

    public Future<MessageObj> requestMessage(String id){
        Promise<MessageObj> promise=Promise.promise();
        vertx.eventBus().<MessageObj>request(MESSAGE_REQUEST_INDEX,id,ar->{
            if (ar.succeeded()){
                promise.complete(ar.result().body());
            }else{
                promise.fail(ar.cause());
            }
        });
        return promise.future();
    }

   public Future<TopicMatchResult> topicMatch(String topic){
       Promise<TopicMatchResult> promise=Promise.promise();
       vertx.eventBus().<TopicMatchResult>request(TOPIC_MATCH,topic,ar->{
           if (ar.succeeded()){
               promise.complete(ar.result().body());
           }else{
               promise.fail(ar.cause());
           }
       });
       return promise.future();
   }

   public void sendMessage(String nodeId,JsonObject message){
        vertx.eventBus().send(SEND_MESSAGE, UnSafeJsonObject.wrapper(new JsonObject().put("nodeId",nodeId).put("body",message)));
   }

   public static class Consumer{
        private Vertx vertx;

       private MessageConsumer<JsonObject> sessionTakenoverConsumer;
       private MessageConsumer<ActionLog> proposalConsumer;
       private MessageConsumer<String> sessionRequestConsumer;
       private MessageConsumer<JsonArray> retainMessageMatchConsumer;
       private MessageConsumer<JsonObject> messageRequestConsumer;
       private MessageConsumer<String> messageRequestIndexConsumer;;

       private MessageConsumer<String> topicMatchConsumer;
       private MessageConsumer<UnSafeJsonObject> sendMessageConsumer;

       public Consumer(Vertx vertx) {
           this.vertx = vertx;
       }

       public Consumer sessionTakenoverHandler(Handler<JsonObject> handler){
           this.sessionTakenoverConsumer=vertx.eventBus().<JsonObject>localConsumer("_session_taken_over_")
                .handler(msg->{
                    JsonObject body = msg.body();
                    handler.handle(body);
                });
           return this;
       }

       public Consumer proposalHandler(Handler<ActionLog> handler){
           this.proposalConsumer=vertx.eventBus().<ActionLog>localConsumer(LOG_PROPOSAL)
                   .handler(msg->{
                       ActionLog body = msg.body();
                       handler.handle(body);
                   });
           return this;
       }

       public Consumer sessionRequestHandler(Handler<Message<String>> handler){
           this.sessionRequestConsumer=vertx.eventBus().<String>localConsumer(SESSION_REQUEST)
                   .handler(handler::handle);
           return this;
       }

       public Consumer retainMessageMatchHandler(Handler<Message<JsonArray>> handler){
           this.retainMessageMatchConsumer=vertx.eventBus().<JsonArray>localConsumer(RETAIN_MESSAGE_MATCH)
                   .handler(handler::handle);
           return this;
       }

       public Consumer messageRequestHandler(Handler<Message<JsonObject>> handler){
           this.messageRequestConsumer=vertx.eventBus().<JsonObject>localConsumer(MESSAGE_REQUEST)
                   .handler(handler::handle);
           return this;
       }


       public Consumer messageRequestIndexHandler(Handler<Message<String>> handler){
           this.messageRequestIndexConsumer=vertx.eventBus().<String>localConsumer(MESSAGE_REQUEST_INDEX)
                   .handler(handler::handle);
           return this;
       }

       public Consumer topicMatchHandler(Handler<Message<String>> handler){
           this.topicMatchConsumer=vertx.eventBus().<String>localConsumer(TOPIC_MATCH)
                   .handler(handler::handle);
           return this;
       }

       public Consumer sendMessageHandler(Handler<UnSafeJsonObject> handler){
           this.sendMessageConsumer=vertx.eventBus().<UnSafeJsonObject>localConsumer(SEND_MESSAGE)
                   .handler(msg->{
                       UnSafeJsonObject body = msg.body();
                       handler.handle(body);
                   });
           return this;
       }

    }


}
