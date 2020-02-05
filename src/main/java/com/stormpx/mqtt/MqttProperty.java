package com.stormpx.mqtt;


import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.stormpx.mqtt.ControlPacketType.*;

public enum  MqttProperty {

    PAYLOAD_FORMAT_INDICATOR((byte)0x01,"byte", PUBLISH,CONNECT),
    MESSAGE_EXPIRY_INTERVAL((byte)0x02,"fourByteInteger",PUBLISH,CONNECT),
    CONTENT_TYPE((byte)0x03,"string",PUBLISH,CONNECT),
    RESPONSE_TOPIC((byte)0x08,"string",PUBLISH,CONNECT),
    CORRELATION_DATA((byte)0x09,"binaryData",PUBLISH,CONNECT),
    SUBSCRIPTION_IDENTIFIER((byte)0x0B,"variableByteInteger",PUBLISH,SUBSCRIBE),
    SESSION_EXPIRY_INTERVAL((byte)0x11,"fourByteInteger",CONNECT,CONNACK,DISCONNECT),
    ASSIGNED_CLIENT_IDENTIFIER((byte)0x12,"string",CONNACK),
    SERVER_KEEP_ALIVE((byte)0x13,"twoByteInteger",CONNACK),
    AUTHENTICATION_METHOD((byte)0x15,"string",CONNECT,CONNACK,AUTH),
    AUTHENTICATION_DATA((byte)0x16,"binaryData",CONNECT,CONNACK,AUTH),
    REQUEST_PROBLEM_INFORMATION((byte)0x17,"byte",CONNECT),
    WILL_DELAY_INTERVAL((byte)0x18,"fourByteInteger",CONNECT),
    REQUEST_RESPONSE_INFORMATION((byte)0x19,"byte",CONNECT),
    RESPONSE_INFORMATION((byte)0x1A,"string",CONNACK),
    SERVER_REFERENCE ((byte)0x1C,"string",CONNACK,DISCONNECT),
    REASON_STRING((byte)0x1F,"string",CONNACK,PUBACK,PUBREC,PUBREL,PUBCOMP,SUBACK,UNSUBACK,DISCONNECT,AUTH),
    RECEIVE_MAXIMUM((byte)0x21,"twoByteInteger",CONNECT,CONNACK),
    TOPIC_ALIAS_MAXIMUM((byte)0x22,"twoByteInteger",CONNECT,CONNACK),
    TOPIC_ALIAS((byte)0x23,"twoByteInteger",PUBLISH),
    MAXIMUM_QOS((byte)0x24,"byte",CONNACK),
    RETAIN_AVAILABLE((byte)0x25,"byte",CONNACK),
    USER_PROPERTY((byte)0x26,"stringPair",CONNECT,CONNACK,PUBLISH,PUBACK,PUBREC,PUBREL,PUBCOMP,SUBSCRIBE,SUBACK,UNSUBSCRIBE,UNSUBACK,DISCONNECT,AUTH),
    MAXIMUM_PACKET_SIZE((byte)0x27,"fourByteInteger",CONNACK,CONNECT),
    WILDCARD_SUBSCRIPTION_AVAILABLE((byte)0x28,"byte",CONNACK),
    SUBSCRIPTION_IDENTIFIER_AVAILABLE((byte)0x29,"byte",CONNACK),
    SHARED_SUBSCRIPTION_AVAILABLE((byte)0x2A,"byte",CONNACK);

    private byte hex;
    private boolean isByte;
    private boolean isFourByteInteger;
    private boolean isVariableByteInteger;
    private boolean isStringPair;
    private boolean isString;
    private boolean isBinaryData;
    private boolean isTwoByteInteger;
    private ControlPacketType[] controlPacketTypes;


    private static final Map<Byte, MqttProperty> VALUE_TO_CODE_MAP;

    static {
        final Map<Byte, MqttProperty> valueMap = new HashMap<>();
        for (MqttProperty code: values()) {
            valueMap.put(code.hex, code);
        }
        VALUE_TO_CODE_MAP = Collections.unmodifiableMap(valueMap);
    }

    MqttProperty(byte hex, String type) {
        this.hex = hex;
        this.isByte = type.equals("byte");
        this.isVariableByteInteger=type.equals("variableByteInteger");
        this.isStringPair =type.equals("stringPair");
        this.isFourByteInteger = type.equals("fourByteInteger");
        this.isString = type.equals("string");
        this.isBinaryData = type.equals("binaryData");
        this.isTwoByteInteger = type.equals("twoByteInteger");
    }

    MqttProperty(byte hex, String type, ControlPacketType... packetTypes) {
        this.hex = hex;
        this.isByte = type.equals("byte");
        this.isVariableByteInteger=type.equals("variableByteInteger");
        this.isStringPair =type.equals("stringPair");
        this.isFourByteInteger = type.equals("fourByteInteger");
        this.isString = type.equals("string");
        this.isBinaryData = type.equals("binaryData");
        this.isTwoByteInteger = type.equals("twoByteInteger");
        this.controlPacketTypes =packetTypes;

    }

    public static MqttProperty valueOf(byte hex){
        MqttProperty property = VALUE_TO_CODE_MAP.get(hex);
        if (property==null)
            throw new IllegalArgumentException(hex+" properties not found");
        return property;
    }

    public static MqttProperty valueOf(byte hex,ControlPacketType packetType){
        MqttProperty property = valueOf(hex);
        for (ControlPacketType type : property.controlPacketTypes) {
            if (type==packetType){
                return property;
            }
        }
        throw new IllegalArgumentException(property+"is not valid for "+packetType);
    }

    public byte byteValue() {
        return hex;
    }

    public boolean isByte() {
        return isByte;
    }

    public boolean isFourByteInteger() {
        return isFourByteInteger;
    }

    public boolean isString() {
        return isString;
    }

    public boolean isBinaryData() {
        return isBinaryData;
    }

    public boolean isTwoByteInteger() {
        return isTwoByteInteger;
    }

    public boolean isVariableByteInteger() {
        return isVariableByteInteger;
    }

    public boolean isStringPair() {
        return isStringPair;
    }


    public boolean useful(ControlPacketType controlPacketType){
        for (ControlPacketType packetType : controlPacketTypes) {
            if (controlPacketType==packetType)
                return true;
        }
        return false;
    }

}
