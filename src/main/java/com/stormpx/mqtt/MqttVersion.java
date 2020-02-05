package com.stormpx.mqtt;

import io.netty.handler.codec.mqtt.MqttUnacceptableProtocolVersionException;

public enum  MqttVersion {
    MQTT_3_1_1( (byte) 4),
    MQTT_5_0( (byte) 5);

    private final byte level;

    MqttVersion( byte protocolLevel) {
        level = protocolLevel;
    }

    public byte protocolLevel() {
        return level;
    }

    public static MqttVersion valueOf(byte protocolLevel) {
        for (MqttVersion mv : values()) {
                if (mv.level == protocolLevel) {
                    return mv;
                }
            }
        throw new MqttUnacceptableProtocolVersionException(protocolLevel + "is unknown protocol version");
    }
}
