package com.stormpx.server;

import com.stormpx.message.MqttAuth;
import com.stormpx.message.MqttWill;

public interface MqttSession {



    String clientIdentifier();

    MqttWill will();

    MqttAuth auth();

    int nextMessageId();

    void use(int packetId);

    void release(int packetId);

    long expiryTime();

    void setExpiryTime();

}
