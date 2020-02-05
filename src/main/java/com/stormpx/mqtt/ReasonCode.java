package com.stormpx.mqtt;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import static com.stormpx.mqtt.ControlPacketType.*;

public enum ReasonCode {

    SUCCESS((byte)0x00, CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK, AUTH,DISCONNECT,SUBACK),
    GRANTED_QOS_1((byte)0x01,SUBACK),
    GRANTED_QOS_2((byte)0x02,SUBACK),
    DISCONNECT_WITH_WILL_MESSAGE((byte)0x04,DISCONNECT),
    NO_MATCHING_SUBSCRIBERS((byte)0x10,PUBACK, PUBREC),
    NO_SUBSCRIPTION_EXISTED((byte)0x11,UNSUBACK),
    CONTINUE_AUTHENTICATION((byte)0x18,AUTH),
    REAUTHENTICATE((byte)0x19,AUTH),
    UNSPECIFIED_ERROR((byte)0x80,CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT),
    MALFORMED_PACKET((byte)0x81,CONNACK, DISCONNECT),
    PROTOCOL_ERROR((byte)0x82,CONNACK, DISCONNECT),
    IMPLEMENTATION_SPECIFIC_ERROR((byte)0x83,CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT),
    UNSUPPORTED_PROTOCOL_VERSION((byte)0x84,CONNACK),
    CLIENT_IDENTIFIER_NOT_VALID((byte)0x85,CONNACK),
    BAD_USER_NAME_OR_PASSWORD((byte)0x86,CONNACK),
    NOT_AUTHORIZED((byte)0x87,CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT),
    SERVER_UNAVAILABLE((byte)0x88,CONNACK),
    SERVER_BUSY((byte)0x89,CONNACK, DISCONNECT),
    BANNED((byte)0x8A,CONNACK),
    SERVER_SHUTTING_DOWN((byte)0x8B,DISCONNECT),
    BAD_AUTHENTICATION_METHOD((byte)0x8C,CONNACK, DISCONNECT),
    KEEP_ALIVE_TIMEOUT((byte)0x8D,DISCONNECT),
    SESSION_TAKEN_OVER((byte)0x8E,DISCONNECT),
    TOPIC_FILTER_INVALID((byte)0x8F,SUBACK, UNSUBACK, DISCONNECT),
    TOPIC_NAME_INVALID((byte)0x90,CONNACK, PUBACK, PUBREC, DISCONNECT),
    PACKET_IDENTIFIER_IN_USE((byte)0x91,PUBACK, PUBREC, SUBACK, UNSUBACK),
    PACKET_IDENTIFIER_NOT_FOUND((byte)0x92,PUBREL, PUBCOMP),
    RECEIVE_MAXIMUM_EXCEEDED((byte)0x93,DISCONNECT),
    TOPIC_ALIAS_INVALID((byte)0x94,DISCONNECT),
    PACKET_TOO_LARGE((byte)0x95,CONNACK, DISCONNECT),
    MESSAGE_RATE_TOO_HIGH((byte)0x96,DISCONNECT),
    QUOTA_EXCEEDED((byte)0x97,CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT),
    ADMINISTRATIVE_ACTION((byte)0x98,DISCONNECT),
    PAYLOAD_FORMAT_INVALID((byte)0x99,CONNACK, PUBACK, PUBREC, DISCONNECT),
    RETAIN_NOT_SUPPORTED((byte)0x9A,CONNACK, DISCONNECT),
    QOS_NOT_SUPPORTED((byte)0x9B,CONNACK, DISCONNECT),
    USE_ANOTHER_SERVER((byte)0x9C,CONNACK, DISCONNECT),
    SERVER_MOVED((byte)0x9D,CONNACK, DISCONNECT),
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED((byte)0x9E,SUBACK, DISCONNECT),
    CONNECTION_RATE_EXCEEDED((byte)0x9F,CONNACK, DISCONNECT),
    MAXIMUM_CONNECT_TIME((byte)0xA0,DISCONNECT),
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED((byte)0xA1,SUBACK, DISCONNECT),
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED((byte)0xA2,SUBACK, DISCONNECT);

    private byte hex;
    private ControlPacketType[] controlPacketTypes;


    private static final Map<Byte, ReasonCode> VALUE_TO_CODE_MAP;

    static {
        final Map<Byte, ReasonCode> valueMap = new HashMap<>();
        for (ReasonCode code: values()) {
            valueMap.put(code.hex, code);
        }
        VALUE_TO_CODE_MAP = Collections.unmodifiableMap(valueMap);
    }

    ReasonCode(byte b, ControlPacketType... packetTypes) {
        this.hex=b;
        this.controlPacketTypes =packetTypes;
    }

    public byte byteValue() {
        return hex;
    }

    public static ReasonCode valueOf(byte hex){

        ReasonCode reasonCode = VALUE_TO_CODE_MAP.get(hex);
        if (reasonCode==null)
            throw new IllegalArgumentException(hex+" reasonCode not found");
        return reasonCode;
    }

    public static ReasonCode valueOf(byte hex,ControlPacketType packetType){
        ReasonCode reasonCode = valueOf(hex);
        for (ControlPacketType type : reasonCode.controlPacketTypes) {
            if (type==packetType){
                return reasonCode;
            }
        }
        throw new IllegalArgumentException(reasonCode+"is not valid for "+packetType);
    }

    public boolean useful(ControlPacketType controlPacketType){
        for (ControlPacketType packetType : controlPacketTypes) {
            if (controlPacketType==packetType)
                return true;
        }
        return false;
    }

}
