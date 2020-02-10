package com.stormpx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;

public class MqttSessionOption {
    private long sessionExpiryInterval;
    private int endPointReceiveMaximum;
    private int receiveMaximum;
    private long endPointMaximumPacketSize;
    private long maximumPacketSize;
    private int endPointTopicAliasMaximum;
    private int topicAliasMaximum;
    private MqttQoS maxQos;
    private boolean requestProblemInformation;
    private boolean retainAvailable;

    public MqttSessionOption() {
        this.sessionExpiryInterval=0;
        this.endPointReceiveMaximum =65535;
        this.receiveMaximum=65535;
        this.endPointMaximumPacketSize =268435455;
        this.maximumPacketSize=268435455;
        this.topicAliasMaximum=0;
        this.endPointTopicAliasMaximum =0;
        this.maxQos=MqttQoS.EXACTLY_ONCE;
        this.requestProblemInformation=false;
        this.retainAvailable=true;
    }


    public long getSessionExpiryInterval() {
        return sessionExpiryInterval;
    }

    public MqttSessionOption setSessionExpiryInterval(long sessionExpiryInterval) {
        this.sessionExpiryInterval = sessionExpiryInterval;
        return this;
    }

    public int getEndPointReceiveMaximum() {
        return endPointReceiveMaximum;
    }

    public MqttSessionOption setEndPointReceiveMaximum(int endPointReceiveMaximum) {
        if (endPointReceiveMaximum >65535)
            endPointReceiveMaximum =65535;
        this.endPointReceiveMaximum = endPointReceiveMaximum;
        return this;
    }


    public int getTopicAliasMaximum() {
        return topicAliasMaximum;
    }

    public MqttSessionOption setTopicAliasMaximum(int topicAliasMaximum) {
        this.topicAliasMaximum = topicAliasMaximum;
        return this;
    }


    public boolean isRequestProblemInformation() {
        return requestProblemInformation;
    }

    public MqttSessionOption setRequestProblemInformation(boolean requestProblemInformation) {
        this.requestProblemInformation = requestProblemInformation;
        return this;
    }


    public long getEndPointMaximumPacketSize() {
        return endPointMaximumPacketSize;
    }

    public MqttSessionOption setEndPointMaximumPacketSize(long endPointMaximumPacketSize) {
        this.endPointMaximumPacketSize = endPointMaximumPacketSize;
        return this;
    }

    public long getMaximumPacketSize() {
        return maximumPacketSize;
    }

    public MqttSessionOption setMaximumPacketSize(long maximumPacketSize) {
        this.maximumPacketSize = maximumPacketSize;
        return this;
    }

    public int getEndPointTopicAliasMaximum() {
        return endPointTopicAliasMaximum;
    }

    public MqttSessionOption setEndPointTopicAliasMaximum(int endPointTopicAliasMaximum) {
        this.endPointTopicAliasMaximum = endPointTopicAliasMaximum;
        return this;
    }

    public boolean isRetainAvailable() {
        return retainAvailable;
    }

    public MqttSessionOption setRetainAvailable(boolean retainAvailable) {
        this.retainAvailable = retainAvailable;
        return this;
    }

    public MqttQoS getMaxQos() {
        return maxQos;
    }

    public MqttSessionOption setMaxQos(MqttQoS maxQos) {
        this.maxQos = maxQos;
        return this;
    }

    public int getReceiveMaximum() {
        return receiveMaximum;
    }

    public MqttSessionOption setReceiveMaximum(int receiveMaximum) {
        this.receiveMaximum = receiveMaximum;
        return this;
    }
}
