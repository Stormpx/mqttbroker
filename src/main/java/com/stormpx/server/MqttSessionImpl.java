package com.stormpx.server;

import com.stormpx.message.MqttAuth;
import com.stormpx.message.MqttWill;
import com.stormpx.mqtt.MqttSessionOption;

import java.time.Instant;
import java.util.HashSet;
import java.util.Set;

public class MqttSessionImpl implements MqttSession {

    private MqttSessionOption sessionOption;
    private String clientIdentifier;
    private MqttWill will;
    private MqttAuth auth;
    private int lastMessageId=1;
    private Set<Integer> set;
    private long expiryTime;


    public MqttSessionImpl(MqttSessionOption sessionOption, String clientIdentifier, MqttWill will, MqttAuth auth) {
        this.sessionOption = sessionOption;
        this.clientIdentifier = clientIdentifier;
        this.will = will;
        this.auth = auth;
        this.set=new HashSet<>();
    }

    public void setClientIdentifier(String clientId){
        this.clientIdentifier=clientId;
    }


    @Override
    public String clientIdentifier() {
        return clientIdentifier;
    }

    @Override
    public MqttWill will() {
        return will;
    }

    @Override
    public MqttAuth auth() {
        return auth;
    }

    @Override
    public int nextMessageId() {
        if (lastMessageId >= 65536)
            lastMessageId = 1;
        //may infinite loop
        while (set.contains(lastMessageId)) {
            lastMessageId+=1;
            if (lastMessageId >= 65536)
                lastMessageId = 1;
        }
        return lastMessageId++;
    }

    @Override
    public void use(int packetId) {
        set.add(packetId);
    }

    @Override
    public void release(int packetId) {
        set.remove(packetId);
    }

    @Override
    public long expiryTime() {
        return expiryTime;
    }

    @Override
    public void setExpiryTime() {
        expiryTime=Instant.now().getEpochSecond() + sessionOption.getSessionExpiryInterval();
    }


}
