package com.stormpx.server;

import io.vertx.core.net.PemKeyCertOptions;

public class MqttServerOption {
    private boolean sni;
    private boolean tcpNoDelay;

    private boolean tcpEnable;
    private String tcpHort;
    private int tcpPort;
    private boolean tcpSsl=false;
    private PemKeyCertOptions tcpPemKeyCertOptions;

    private boolean wsEnable;
    private String wsPath;
    private String wsHort;
    private int wsPort;
    private boolean wsSsl=false;
    private String wsKeyPath;
    private String wsCertPath;
    private PemKeyCertOptions wsPemKeyCertOptions;

    public boolean isSni() {
        return sni;
    }

    public MqttServerOption setSni(boolean sni) {
        this.sni = sni;
        return this;
    }

    public boolean isTcpNoDelay() {
        return tcpNoDelay;
    }

    public MqttServerOption setTcpNoDelay(boolean tcpNoDelay) {
        this.tcpNoDelay = tcpNoDelay;
        return this;
    }

    public boolean isTcpEnable() {
        return tcpEnable;
    }

    public MqttServerOption setTcpEnable(boolean tcpEnable) {
        this.tcpEnable = tcpEnable;
        return this;
    }

    public String getTcpHort() {
        return tcpHort;
    }

    public MqttServerOption setTcpHort(String tcpHort) {
        this.tcpHort = tcpHort;
        return this;
    }

    public int getTcpPort() {
        return tcpPort;
    }

    public MqttServerOption setTcpPort(int tcpPort) {
        this.tcpPort = tcpPort;
        return this;
    }

    public boolean isTcpSsl() {
        return tcpSsl;
    }

    public MqttServerOption setTcpSsl(boolean tcpSsl) {
        this.tcpSsl = tcpSsl;
        return this;
    }


    public boolean isWsEnable() {
        return wsEnable;
    }

    public MqttServerOption setWsEnable(boolean wsEnable) {
        this.wsEnable = wsEnable;
        return this;
    }

    public String getWsPath() {
        return wsPath;
    }

    public MqttServerOption setWsPath(String wsPath) {
        this.wsPath = wsPath;
        return this;
    }

    public String getWsHort() {
        return wsHort;
    }

    public MqttServerOption setWsHort(String wsHort) {
        this.wsHort = wsHort;
        return this;
    }

    public int getWsPort() {
        return wsPort;
    }

    public MqttServerOption setWsPort(int wsPort) {
        this.wsPort = wsPort;
        return this;
    }

    public boolean isWsSsl() {
        return wsSsl;
    }

    public MqttServerOption setWsSsl(boolean wsSsl) {
        this.wsSsl = wsSsl;
        return this;
    }

    public String getWsKeyPath() {
        return wsKeyPath;
    }

    public MqttServerOption setWsKeyPath(String wsKeyPath) {
        this.wsKeyPath = wsKeyPath;
        return this;
    }

    public String getWsCertPath() {
        return wsCertPath;
    }

    public MqttServerOption setWsCertPath(String wsCertPath) {
        this.wsCertPath = wsCertPath;
        return this;
    }

    public PemKeyCertOptions getTcpPemKeyCertOptions() {
        return tcpPemKeyCertOptions;
    }

    public MqttServerOption setTcpPemKeyCertOptions(PemKeyCertOptions tcpPemKeyCertOptions) {
        this.tcpPemKeyCertOptions = tcpPemKeyCertOptions;
        return this;
    }

    public PemKeyCertOptions getWsPemKeyCertOptions() {
        return wsPemKeyCertOptions;
    }

    public MqttServerOption setWsPemKeyCertOptions(PemKeyCertOptions wsPemKeyCertOptions) {
        this.wsPemKeyCertOptions = wsPemKeyCertOptions;
        return this;
    }
}
