package com.stormpx.mqtt;

import com.stormpx.kit.MqttCodecUtil;
import com.stormpx.kit.StringPair;
import io.netty.buffer.ByteBuf;

import java.util.*;

public class PropertiesEncoder {
    private ControlPacketType controlPacketType;
    private List<MqttProperties> properties;
    private int length;
    private List<MqttProperties> optionalProperties;

    public PropertiesEncoder(ControlPacketType controlPacketType, List<MqttProperties> properties) {
        this.controlPacketType = controlPacketType;
        this.properties = properties==null?null:new LinkedList<>(properties);
        this.optionalProperties =new LinkedList<>();
        init();
    }

    /**
     * CONNACK PUBACK PUBREC PUBREL PUBCOMP SUBACK UNSUBACK DISCONNECT AUTH
     * reasonString userProperty
     * MUST NOT send this property if it would increase the size of the packet
     * beyond the Maximum Packet Size specified by the receiver.
     */
    private void init(){
        this.length=0;
        if (properties==null)
            return;
        for (MqttProperties mqttProperties : properties) {
            MqttProperty property = mqttProperties.getProperty();
            switch (controlPacketType){
                case CONNACK:
                case PUBACK:
                case PUBREC:
                case PUBREL:
                case PUBCOMP:
                case SUBACK:
                case UNSUBACK:
                case DISCONNECT:
                case AUTH:
                    if (property==MqttProperty.REASON_STRING||property==MqttProperty.USER_PROPERTY){
                        optionalProperties.add(mqttProperties);
                    }
            }
            length+=mqttProperties.getCodecLength();
        }
    }


    /**
     * del optional properties
     * @param properties
     */
    public void del(MqttProperties properties){
        Iterator<MqttProperties> iterator = this.properties.iterator();
        while (iterator.hasNext()){
            MqttProperties mqttProperties = iterator.next();
            if (mqttProperties.equals(properties)){
                length-=mqttProperties.getCodecLength();
                iterator.remove();
                return;
            }
        }
    }

    public int getTotalLength() {
        return length;
    }

    public List<MqttProperties> getOptionalProperties() {
        return optionalProperties;
    }

    public void encode(ByteBuf buffer) {

        for (MqttProperties mqttProperties : properties) {
            MqttProperty property = mqttProperties.getProperty();
            Object value = mqttProperties.getValue();
            buffer.writeByte(property.byteValue());

            if (property.isByte()){
                buffer.writeByte((byte) value);
            }else if (property.isBinaryData()){
                ByteBuf data= (ByteBuf) value;
                buffer.writeShort(data.readableBytes());
                buffer.writeBytes(data);
            }else if (property.isFourByteInteger()){
                buffer.writeInt(((Long) value).intValue());
            }else if (property.isString()){
                byte[] string = MqttCodecUtil.encodeUtf8String(value.toString());
                buffer.writeShort(string.length);
                buffer.writeBytes(string);
            }else if (property.isStringPair()){
                StringPair pair= (StringPair) value;
                byte[] keyBytes = MqttCodecUtil.encodeUtf8String(pair.getKey());
                buffer.writeShort(keyBytes.length);
                buffer.writeBytes(keyBytes);
                byte[] valueBytes = MqttCodecUtil.encodeUtf8String(pair.getValue());
                buffer.writeShort(valueBytes.length);
                buffer.writeBytes(valueBytes);
            }else if (property.isTwoByteInteger()){
                buffer.writeShort((Integer) value);
            }else if (property.isVariableByteInteger()){
                buffer.writeBytes(MqttCodecUtil.encodeVariableLength((Integer) value));
            }

        }
    }
}
