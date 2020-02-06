package com.stormpx.message;

import io.vertx.core.buffer.Buffer;
import io.vertx.core.net.SocketAddress;

public class MqttAuth {

    private String userName;
    private SocketAddress socketAddress;
    private Buffer password;
    private String authMethod;
    private Buffer authData;

    public MqttAuth(String userName, SocketAddress socketAddress, Buffer password, String authMethod, Buffer authData) {
        this.userName = userName;
        this.socketAddress = socketAddress;
        this.password = password;
        this.authMethod = authMethod;
        this.authData = authData;
    }


    public SocketAddress getSocketAddress() {
        return socketAddress;
    }

    public String getUserName() {
        return userName;
    }

    public Buffer getPassword() {
        return password;
    }

    public String getAuthMethod() {
        return authMethod;
    }

    public Buffer getAuthData() {
        return authData;
    }
}
