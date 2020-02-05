package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttSubscription;

import java.util.List;
import java.util.Objects;

public class MqttSubscribePacket implements MqttPacket{
    private FixedHeader fixedHeader;

    private int packetIdentifier ;
    private List<MqttProperties> properties ;
    private List<MqttSubscription> subscriptions;

    public MqttSubscribePacket(FixedHeader fixedHeader, int packetIdentifier, List<MqttProperties> properties, List<MqttSubscription> subscriptions) {
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

    public List<MqttSubscription> getSubscriptions() {
        return subscriptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttSubscribePacket that = (MqttSubscribePacket) o;
        return packetIdentifier == that.packetIdentifier && Objects.equals(fixedHeader, that.fixedHeader) && Objects.equals(properties, that.properties) && Objects.equals(subscriptions, that.subscriptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedHeader, packetIdentifier, properties, subscriptions);
    }

    @Override
    public String toString() {
        return "MqttSubscribePacket{" + "fixedHeader=" + fixedHeader + ", packetIdentifier=" + packetIdentifier + ", properties=" + properties + ", subscriptions=" + subscriptions + '}';
    }
}
