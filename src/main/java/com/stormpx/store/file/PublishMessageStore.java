package com.stormpx.store.file;

import com.stormpx.store.MessageObj;
import io.vertx.core.json.JsonObject;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class PublishMessageStore {
    private Map<String, MessageObj> messageMap;

    public PublishMessageStore() {
        this.messageMap = new ConcurrentHashMap<>();
    }

    public void put(String id,MessageObj messageObj){
        messageMap.put(id,messageObj);
    }

    public void put(String id, JsonObject message){
        messageMap.put(id,new MessageObj(message));
    }

    public MessageObj getObj(String id){
        MessageObj messageObj = messageMap.get(id);
        return messageObj;
    }

    public JsonObject get(String id){
        MessageObj messageObj = messageMap.get(id);
        if (messageObj==null)
            return null;

        JsonObject message = messageObj.getMessage();

        return message;
    }

    public void add(String id,int cnf){
        MessageObj messageObj = getObj(id);
        if (messageObj==null)
            return;

        int c = messageObj.add(cnf);
        if (c<=0)
            remove(id);

    }

    public void remove(String id){
        messageMap.remove(id);
    }
    public Set<String> keys(){
        return messageMap.keySet();
    }


}
