package com.stormpx.cluster;

import com.stormpx.cluster.message.ActionLog;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.MessageObj;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class Cluster {
    private Vertx vertx;



    public Cluster(Vertx vertx) {
        this.vertx = vertx;
    }

    public Consumer consumer(){
        return new Consumer(vertx);
    }




    public void proposal(ActionLog actionLog){
        vertx.eventBus().send("_log_proposal_",actionLog);
    }

   public Future<SessionResult> requestSession(String clientId){
       Promise<SessionResult> promise=Promise.promise();
        vertx.eventBus().<SessionResult>request("_session_request_",clientId,ar->{
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
       vertx.eventBus().<RetainMatchResult>request("_retain_message_match_",new JsonArray(topicFilters), ar->{
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
       vertx.eventBus().<MessageObj>request("_message_request_",new JsonObject().put("nodeIds",new ArrayList<>(nodeIds)).put("id",id),ar->{
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
        vertx.eventBus().<MessageObj>request("_message_request_index_",id,ar->{
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
       vertx.eventBus().<TopicMatchResult>request("_topic_match_",topic,ar->{
           if (ar.succeeded()){
               promise.complete(ar.result().body());
           }else{
               promise.fail(ar.cause());
           }
       });
       return promise.future();
   }

   public void sendMessage(String nodeId,JsonObject message){
        vertx.eventBus().send("_send_message_", UnSafeJsonObject.wrapper(new JsonObject().put("nodeId",nodeId).put("body",message)));
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
           this.proposalConsumer=vertx.eventBus().<ActionLog>localConsumer("_log_proposal_")
                   .handler(msg->{
                       ActionLog body = msg.body();
                       handler.handle(body);
                   });
           return this;
       }

       public Consumer sessionRequestHandler(Handler<Message<String>> handler){
           this.sessionRequestConsumer=vertx.eventBus().<String>localConsumer("_session_request_")
                   .handler(handler::handle);
           return this;
       }

       public Consumer retainMessageMatchHandler(Handler<Message<JsonArray>> handler){
           this.retainMessageMatchConsumer=vertx.eventBus().<JsonArray>localConsumer("_retain_message_match_")
                   .handler(handler::handle);
           return this;
       }

       public Consumer messageRequestHandler(Handler<Message<JsonObject>> handler){
           this.messageRequestConsumer=vertx.eventBus().<JsonObject>localConsumer("_message_request_")
                   .handler(msg->{
                       handler.handle(msg);
                   });
           return this;
       }


       public Consumer messageRequestIndexHandler(Handler<Message<String>> handler){
           this.messageRequestIndexConsumer=vertx.eventBus().<String>localConsumer("_message_request_index")
                   .handler(msg->{
                       handler.handle(msg);
                   });
           return this;
       }

       public Consumer topicMatchHandler(Handler<Message<String>> handler){
           this.topicMatchConsumer=vertx.eventBus().<String>localConsumer("_topic_match_")
                   .handler(msg->{
                       handler.handle(msg);
                   });
           return this;
       }

       public Consumer sendMessageHandler(Handler<UnSafeJsonObject> handler){
           this.sendMessageConsumer=vertx.eventBus().<UnSafeJsonObject>localConsumer("_send_message_")
                   .handler(msg->{
                       UnSafeJsonObject body = msg.body();
                       handler.handle(body);
                   });
           return this;
       }

    }


}
