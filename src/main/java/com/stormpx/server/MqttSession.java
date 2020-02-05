package com.stormpx.server;

import com.stormpx.broker.MqttAuth;
import com.stormpx.broker.MqttWill;

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
