package com.stormpx.mqtt.packet;

import com.stormpx.mqtt.FixedHeader;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttVersion;
import io.netty.buffer.ByteBuf;
import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.List;
import java.util.Objects;

public class MqttConnectPacket implements MqttPacket {
    private FixedHeader fixedHeader;
    private MqttVersion version;
    private String ClientIdentifier;
    private boolean cleanStart;
    private int keepAlive;
    private List<MqttProperties> properties;
    private boolean willFlag;
    private boolean willRetain;
    private List<MqttProperties> willProperties;
    private MqttQoS willQos;
    private String willTopic;
    private ByteBuf willPayload;
    private String userName;
    private ByteBuf password;

    public MqttConnectPacket(FixedHeader fixedHeader, MqttVersion version, String clientIdentifier, boolean cleanStart, int keepAlive, List<MqttProperties> properties, boolean willFlag, boolean willRetain, List<MqttProperties> willProperties, MqttQoS willQos, String willTopic, ByteBuf willPayload, String userName, ByteBuf password) {
        this.fixedHeader = fixedHeader;
        this.version = version;
        ClientIdentifier = clientIdentifier;
        this.cleanStart = cleanStart;
        this.keepAlive = keepAlive;
        this.properties = properties;
        this.willFlag = willFlag;
        this.willRetain = willRetain;
        this.willProperties = willProperties;
        this.willQos = willQos;
        this.willTopic = willTopic;
        this.willPayload = willPayload;
        this.userName = userName;
        this.password = password;
    }

    @Override
    public FixedHeader fixedHeader() {
        return fixedHeader;
    }

    public MqttVersion getVersion() {
        return version;
    }

    public String getClientIdentifier() {
        return ClientIdentifier;
    }

    public boolean isCleanStart() {
        return cleanStart;
    }

    public int getKeepAlive() {
        return keepAlive;
    }

    public List<MqttProperties> getProperties() {
        return properties;
    }

    public boolean isWillFlag() {
        return willFlag;
    }

    public boolean isWillRetain() {
        return willRetain;
    }

    public List<MqttProperties> getWillProperties() {
        return willProperties;
    }

    public MqttQoS getWillQos() {
        return willQos;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public ByteBuf getWillPayload() {
        return willPayload;
    }

    public String getUserName() {
        return userName;
    }

    public ByteBuf getPassword() {
        return password;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttConnectPacket that = (MqttConnectPacket) o;
        return cleanStart == that.cleanStart && keepAlive == that.keepAlive && willFlag == that.willFlag && willRetain == that.willRetain && version == that.version && Objects.equals(ClientIdentifier, that.ClientIdentifier) && Objects.equals(properties, that.properties) && Objects.equals(willProperties, that.willProperties) && willQos == that.willQos && Objects.equals(willTopic, that.willTopic) && Objects.equals(willPayload, that.willPayload) && Objects.equals(userName, that.userName) && Objects.equals(password, that.password);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, ClientIdentifier, cleanStart, keepAlive, properties, willFlag, willRetain, willProperties, willQos, willTopic, willPayload, userName, password);
    }

    @Override
    public String toString() {
        return "MqttConnectPacket{" + "fixedHeader=" + fixedHeader + ", version=" + version + ", ClientIdentifier='" + ClientIdentifier + '\'' + ", cleanStart=" + cleanStart + ", keepAlive=" + keepAlive + ", properties=" + properties + ", willFlag=" + willFlag + ", willRetain=" + willRetain + ", willProperties=" + willProperties + ", willQos=" + willQos + ", willTopic='" + willTopic + '\'' + ", willPayload=" + willPayload + ", userName='" + userName + '\'' + ", password=" + password + '}';
    }
}
