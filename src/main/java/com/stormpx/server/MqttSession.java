package com.stormpx.server;

import com.stormpx.message.MqttAuth;
import com.stormpx.message.MqttWill;

public interface MqttSession {



    String clientIdentifier();

    MqttWill will();

    MqttAuth auth();

    int nextMessageId();

    void send(int packetId);

    void release(int packetId);

    void addPacketId(int packetId);

    boolean containsPacketId(int packetId);

    void removePacketId(int packetId);

    int endPointUsedQuota();

    long expiryTime();

    void setExpiryTime();

}
