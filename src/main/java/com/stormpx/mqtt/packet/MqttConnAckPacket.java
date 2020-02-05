package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;

import java.util.List;
import java.util.Objects;

public class MqttConnAckPacket implements MqttPacket {

    private FixedHeader fixedHeader;
    private byte code ;
    private List<MqttProperties> properties ;
    private boolean sessionPresent;

    public MqttConnAckPacket(FixedHeader fixedHeader, byte code, List<MqttProperties> properties, boolean sessionPresent) {
        this.fixedHeader = fixedHeader;
        this.code = code;
        this.properties = properties;
        this.sessionPresent = sessionPresent;
    }

    @Override
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    public byte getCode() {
        return code;
    }

    public List<MqttProperties> getProperties() {
        return properties;
    }

    public boolean isSessionPresent() {
        return sessionPresent;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttConnAckPacket packet = (MqttConnAckPacket) o;
        return code == packet.code && sessionPresent == packet.sessionPresent && Objects.equals(fixedHeader, packet.fixedHeader) && Objects.equals(properties, packet.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedHeader, code, properties, sessionPresent);
    }

    @Override
    public String toString() {
        return "MqttConnAckPacket{" + "fixedHeader=" + fixedHeader + ", code=" + code + ", properties=" + properties + ", sessionPresent=" + sessionPresent + '}';
    }
}
