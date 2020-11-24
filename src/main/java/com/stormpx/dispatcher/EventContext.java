package com.stormpx.dispatcher;

import com.stormpx.mqtt.MqttSubscription;

import java.util.List;

public class EventContext {
    //1:packetid 2: message 3: subscribe
    private int type;

    private Object body;

    public static EventContext packetIds(PacketIds packetIds){
        var e=new EventContext();
        e.type=1;
        e.body=packetIds;
        return e;
    }
    public static EventContext message(MessageContext messageContext){
        var e=new EventContext();
        e.type=2;
        e.body=messageContext;
        return e;
    }
    public static EventContext subscription(List<MqttSubscription> subscriptionGroups){
        var e=new EventContext();
        e.type=3;
        e.body=subscriptionGroups;
        return e;
    }



    public boolean isPacketId(){
        return type ==1;
    }

    public boolean isMessage(){
        return type==2;
    }
    public boolean isSubscribe(){
        return type==3;
    }


    public PacketIds asPacketIds(){
        return (PacketIds) body;
    }

    public MessageContext asMessage(){
        return (MessageContext) body;
    }

    public List<MqttSubscription> asSubscribeList(){
        return (List<MqttSubscription>) body;
    }

}
