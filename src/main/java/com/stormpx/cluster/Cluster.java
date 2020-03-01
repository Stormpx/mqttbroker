package com.stormpx.cluster;

import com.stormpx.cluster.message.ActionLog;
import com.stormpx.kit.UnSafeJsonObject;
import com.stormpx.store.MessageObj;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
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

}
