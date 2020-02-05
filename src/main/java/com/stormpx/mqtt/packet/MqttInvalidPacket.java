package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;

public class MqttInvalidPacket implements MqttPacket{
    private FixedHeader fixedHeader;
    private Throwable cause;

    public MqttInvalidPacket(FixedHeader fixedHeader, Throwable cause) {
        this.fixedHeader = fixedHeader;
        this.cause = cause;
    }

    @Override
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    @Override
    public Throwable cause() {
        return cause;
    }

    @Override
    public String toString() {
        return "MqttInvalidPacket{" + "fixedHeader=" + fixedHeader + ", cause=" + cause + '}';
    }
}
