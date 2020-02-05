package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.ReasonCode;

import java.util.List;
import java.util.Objects;

public class MqttUnsubOrSubAckPacket implements MqttPacket{
    private FixedHeader fixedHeader;
    private int packetIdentifier;
    private List<MqttProperties> properties;
    private List<ReasonCode> reasonCodes;

    public MqttUnsubOrSubAckPacket(FixedHeader fixedHeader, int packetIdentifier, List<MqttProperties> properties, List<ReasonCode> reasonCodes) {
        this.fixedHeader = fixedHeader;
        this.packetIdentifier = packetIdentifier;
        this.properties = properties;
        this.reasonCodes = reasonCodes;
    }

    @Override
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    public int getPacketIdentifier() {
        return packetIdentifier;
    }

    public List<MqttProperties> getProperties() {
        return properties;
    }

    public List<ReasonCode> getReasonCodes() {
        return reasonCodes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttUnsubOrSubAckPacket that = (MqttUnsubOrSubAckPacket) o;
        return packetIdentifier == that.packetIdentifier && Objects.equals(fixedHeader, that.fixedHeader) && Objects.equals(properties, that.properties) && Objects.equals(reasonCodes, that.reasonCodes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedHeader, packetIdentifier, properties, reasonCodes);
    }

    @Override
    public String toString() {
        return "MqttUnsubOrSubAckPacket{" + "fixedHeader=" + fixedHeader + ", packetIdentifier=" + packetIdentifier + ", properties=" + properties + ", reasonCodes=" + reasonCodes + '}';
    }
}
