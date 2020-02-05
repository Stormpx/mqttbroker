package com.stormpx.mqtt;


public enum  ControlPacketType {

    CONNECT(1),
    CONNACK(2),
    PUBLISH(3),
    PUBACK(4),
    PUBREC(5),
    PUBREL(6),
    PUBCOMP(7),
    SUBSCRIBE(8),
    SUBACK(9),
    UNSUBSCRIBE(10),
    UNSUBACK(11),
    PINGREQ(12),
    PINGRESP(13),
    DISCONNECT(14),
    AUTH(15);

    private final int value;

    ControlPacketType(int value) {
        this.value = value;
    }

    public int value() {
        return value;
    }

    public static ControlPacketType valueOf(int type) {
        for (ControlPacketType t : values()) {
            if (t.value == type) {
                return t;
            }
        }
        throw new IllegalArgumentException("unknown packet type: " + type);
    }
}
