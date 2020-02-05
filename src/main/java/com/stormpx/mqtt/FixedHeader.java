package com.stormpx.mqtt;

import io.netty.handler.codec.mqtt.MqttQoS;

import java.util.Objects;

public class FixedHeader {

    public final static FixedHeader CONNECT=new FixedHeader(ControlPacketType.CONNECT, false, 0, false, 0);
    public final static FixedHeader CONNACK=new FixedHeader(ControlPacketType.CONNACK,false,0,false,0);
    public final static FixedHeader PUBACK=new FixedHeader(ControlPacketType.PUBACK,false,0,false,0);
    public final static FixedHeader PUBREC=new FixedHeader(ControlPacketType.PUBREC,false,0,false,0);
    public final static FixedHeader PUBREL=new FixedHeader(ControlPacketType.PUBREL,false,1,false,0);
    public final static FixedHeader PUBCOMP=new FixedHeader(ControlPacketType.PUBCOMP, false, 0, false, 0);
    public final static FixedHeader SUBSCRIBE=new FixedHeader(ControlPacketType.SUBSCRIBE, false, 1, false, 0);
    public final static FixedHeader SUBACK=new FixedHeader(ControlPacketType.SUBACK, false, 0, false, 0);
    public final static FixedHeader UNSUBSCRIBE=new FixedHeader(ControlPacketType.UNSUBSCRIBE, false, 1, false, 0);
    public final static FixedHeader UNSUBACK=new FixedHeader(ControlPacketType.UNSUBACK, false, 0, false, 0);
    public final static FixedHeader PINGREQ=new FixedHeader(ControlPacketType.PINGREQ, false, 0, false, 0);
    public final static FixedHeader PINGRESP=new FixedHeader(ControlPacketType.PINGRESP, false, 0, false, 0);
    public final static FixedHeader DISCONNECT=new FixedHeader(ControlPacketType.DISCONNECT, false, 0, false, 0);
    public final static FixedHeader AUTH=new FixedHeader(ControlPacketType.AUTH, false, 0, false, 0);

    private ControlPacketType packetType;
    private boolean dup;
    private int qos;
    private boolean retain;
    private int remainingLength;

    public FixedHeader(ControlPacketType packetType, boolean dup, int qos, boolean retain, int remainingLength) {
        this.packetType = packetType;
        this.dup = dup;
        this.qos =qos;
        this.retain = retain;
        this.remainingLength = remainingLength;
    }

    public static FixedHeader PUBLISH(boolean dup, MqttQoS qos, boolean retain){
        return new FixedHeader(ControlPacketType.PUBLISH,dup,qos.value(),retain,0);
    }


    public ControlPacketType getPacketType() {
        return packetType;
    }

    public boolean isDup() {
        return dup;
    }

    public int getQos() {
        return qos;
    }

    public MqttQoS getQosAsEnum() {
        return MqttQoS.valueOf(qos);
    }

    public boolean isRetain() {
        return retain;
    }

    public int getRemainingLength() {
        return remainingLength;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FixedHeader that = (FixedHeader) o;
        return dup == that.dup && qos == that.qos && retain == that.retain && packetType == that.packetType;
    }

    @Override
    public int hashCode() {
        return Objects.hash(packetType, dup, qos, retain);
    }

    @Override
    public String toString() {
        return "FixedHeader{" + "packetType=" + packetType + ", dup=" + dup + ", qos=" + qos + ", retain=" + retain + ", remainingLength=" + remainingLength + '}';
    }
}
