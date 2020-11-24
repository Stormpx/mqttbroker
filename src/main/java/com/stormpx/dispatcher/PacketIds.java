package com.stormpx.dispatcher;

import java.util.Set;

public class PacketIds {

    //1: unReceived 2: unConfirm
    private int state;

    private Set<Integer> id;

    private PacketIds( Set<Integer> id) {
        this.id = id;
    }

    public static PacketIds unReceivedId(Set<Integer> id){
        PacketIds packetIds = new PacketIds(id);
        packetIds.state=1;
        return packetIds;
    }

    public static PacketIds unConfirmId(Set<Integer> id){
        PacketIds packetIds = new PacketIds(id);
        packetIds.state=2;
        return packetIds;
    }

    public boolean isUnReceived(){
        return state==1;
    }

    public boolean isUnConfirm(){
        return state==2;
    }


    public Set<Integer> getId() {
        return id;
    }
}
