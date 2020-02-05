package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.ReasonCode;

import java.util.List;
import java.util.Objects;

public class MqttPapaPacket implements MqttPacket {
    private FixedHeader fixedHeader;
    private int packetIdentifier;
    private ReasonCode reasonCode;
    private List<MqttProperties> properties;

    public MqttPapaPacket(FixedHeader fixedHeader, int packetIdentifier, ReasonCode reasonCode, List<MqttProperties> properties) {
        this.fixedHeader = fixedHeader;
        this.packetIdentifier = packetIdentifier;
        this.reasonCode = reasonCode;
        this.properties = properties;
    }

    @Override
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    public int getPacketIdentifier() {
        return packetIdentifier;
    }

    public ReasonCode getReasonCode() {
        return reasonCode;
    }

    public List<MqttProperties> getProperties() {
        return properties;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttPapaPacket that = (MqttPapaPacket) o;
        return packetIdentifier == that.packetIdentifier && Objects.equals(fixedHeader, that.fixedHeader) && reasonCode == that.reasonCode && Objects.equals(properties, that.properties);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedHeader, packetIdentifier, reasonCode, properties);
    }

    @Override
    public String toString() {
        return "MqttPapaPacket{" + "fixedHeader=" + fixedHeader + ", packetIdentifier=" + packetIdentifier + ", reasonCode=" + reasonCode + ", properties=" + properties + '}';
    }
}
