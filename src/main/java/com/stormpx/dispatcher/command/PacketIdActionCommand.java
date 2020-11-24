package com.stormpx.dispatcher.command;

public class PacketIdActionCommand {

    //1: discard 2: release 3: addPacketId 4: removePacketId
    private int action;
    private String clientId;
    private int packetId;


    private PacketIdActionCommand(int action, String clientId, int packetId) {
        this.action = action;
        this.clientId = clientId;
        this.packetId = packetId;
    }

    public static PacketIdActionCommand discardMessage(String clientId, int packetId){
        return new PacketIdActionCommand(1,clientId,packetId);
    }

    public static PacketIdActionCommand release(String clientId, int packetId){
        return new PacketIdActionCommand(2,clientId,packetId);
    }
    public static PacketIdActionCommand addPacketId(String clientId, int packetId){
        return new PacketIdActionCommand(3,clientId,packetId);
    }
    public static PacketIdActionCommand removePacketId(String clientId, int packetId){
        return new PacketIdActionCommand(4,clientId,packetId);
    }

    public boolean isDiscard(){
        return action==1;
    }
    public boolean isRelease(){
        return action==2;
    }
    public boolean isAddPacketId(){
        return action==3;
    }
    public boolean isRemovePacketId(){
        return action==4;
    }



    public String getClientId() {
        return clientId;
    }

    public int getPacketId() {
        return packetId;
    }

    private String action(){
        switch (action){
            case 1:
                return "discard";
            case 2:
                return "release";
            case 3:
                return "addPacketId";
            case 4:
                return "removePacketId";
        }
        return "unknown";
    }

    @Override
    public String toString() {
        return "PacketIdAction{" + "action=" + action() + ", clientId='" + clientId + '\'' + ", packetId=" + packetId + '}';
    }
}
