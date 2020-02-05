package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;

import java.util.List;
import java.util.Objects;

public class MqttUnSubscribePacket implements MqttPacket{
    private FixedHeader fixedHeader;

    private int packetIdentifier ;
    private List<MqttProperties> properties ;
    private List<String> subscriptions;


    public MqttUnSubscribePacket(FixedHeader fixedHeader, int packetIdentifier, List<MqttProperties> properties, List<String> subscriptions) {
        this.fixedHeader = fixedHeader;
        this.packetIdentifier = packetIdentifier;
        this.properties = properties;
        this.subscriptions = subscriptions;
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

    public List<String> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttUnSubscribePacket that = (MqttUnSubscribePacket) o;
        return packetIdentifier == that.packetIdentifier && Objects.equals(fixedHeader, that.fixedHeader) && Objects.equals(properties, that.properties) && Objects.equals(subscriptions, that.subscriptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedHeader, packetIdentifier, properties, subscriptions);
    }

    @Override
    public String toString() {
        return "MqttUnSubscribePacket{" + "fixedHeader=" + fixedHeader + ", packetIdentifier=" + packetIdentifier + ", properties=" + properties + ", subscriptions=" + subscriptions + '}';
    }
}
