package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;
import io.netty.buffer.ByteBuf;

import java.util.List;
import java.util.Objects;

public class MqttPublishPacket implements MqttPacket{
    private FixedHeader fixedHeader;

    private String topicName;
    private Integer packetIdentifier;
    private List<MqttProperties> properties;
    private ByteBuf payload;

    public MqttPublishPacket(FixedHeader fixedHeader, String topicName, Integer packetIdentifier, List<MqttProperties> properties, ByteBuf payload) {
        this.fixedHeader = fixedHeader;
        this.topicName = topicName;
        this.packetIdentifier = packetIdentifier;
        this.properties = properties;
        this.payload = payload;
    }

    @Override
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    public String getTopicName() {
        return topicName;
    }

    public Integer getPacketIdentifier() {
        return packetIdentifier;
    }

    public List<MqttProperties> getProperties() {
        return properties;
    }

    public ByteBuf getPayload() {
        return payload;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttPublishPacket that = (MqttPublishPacket) o;
        return Objects.equals(fixedHeader, that.fixedHeader) && Objects.equals(topicName, that.topicName) && Objects.equals(packetIdentifier, that.packetIdentifier) && Objects.equals(properties, that.properties) && Objects.equals(payload, that.payload);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedHeader, topicName, packetIdentifier, properties, payload);
    }

    @Override
    public String toString() {
        return "MqttPublishPacket{" + "fixedHeader=" + fixedHeader + ", topicName='" + topicName + '\'' + ", packetIdentifier=" + packetIdentifier + ", properties=" + properties + ", payload=" + payload + '}';
    }
}
