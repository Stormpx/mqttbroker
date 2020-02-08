package com.stormpx.mqtt;

import com.stormpx.kit.StringPair;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.json.JsonObject;

import java.util.Objects;

public class MqttProperties {
    private MqttProperty property;
    private Object value;

    public MqttProperties(MqttProperty property, Object value) {
        this.property = property;
        this.value = value;
    }
    public MqttProperties(MqttProperty property,String value) {
        this.property = property;
        this.value=value;
    }

    public MqttProperties(MqttProperty property,Integer value) {
        this.property = property;
        this.value=value;
    }
    public MqttProperties(MqttProperty property,Long value) {
        this.property = property;
        this.value=value;
    }
    public MqttProperties(MqttProperty property,byte value) {
        this.property = property;
        this.value=value;
    }
    public MqttProperties(MqttProperty property,StringPair value) {
        this.property = property;
        this.value=value;
    }
    public MqttProperties(MqttProperty property,ByteBuf value) {
        this.property = property;
        this.value=value;
    }

    public MqttProperty getProperty() {
        return property;
    }


    public Object getValue() {
        return value;
    }

    public JsonObject toJson(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("property",property.byteValue());
        if (property.isBinaryData()){
            ByteBuf byteBuf= (ByteBuf) value;
            byte[] bytes = new byte[byteBuf.readableBytes()];
            byteBuf.getBytes(byteBuf.readerIndex(),bytes);
            jsonObject.put("value",bytes);
        }else if (property.isStringPair()){
            StringPair stringPair= (StringPair) value;
            jsonObject.put("key",stringPair.getKey());
            jsonObject.put("value",stringPair.getValue());
        }else{
            jsonObject.put("value",value);
        }
        return jsonObject;
    }

    public static MqttProperties fromJson(JsonObject json){
        Integer property = json.getInteger("property");
        if (property==null)
            throw new NullPointerException("key:property is null");
        MqttProperty mqttProperty = MqttProperty.valueOf(property.byteValue());
        Object value=null;
        if (mqttProperty.isBinaryData()){

            byte[] values = json.getBinary("value");
            if (values!=null)
                value= Unpooled.wrappedBuffer(values);

        }else if (mqttProperty.isStringPair()){

            String key = json.getString("key");
            String v = json.getString("value");
            if (key==null||v==null)
                throw new NullPointerException("string pair is null");
            value=new StringPair(key,v);

        }else if (mqttProperty.isByte()){

            Integer i = json.getInteger("value");
            if (i==null)
                throw new NullPointerException("value is null");
            value=i.byteValue();

        }else if (mqttProperty.isString()){
            value=json.getString("value");
        }else if (mqttProperty.isFourByteInteger()){
            value=json.getLong("value");
        }else if (mqttProperty.isTwoByteInteger()){
            value=json.getInteger("value");
        }else if (mqttProperty.isVariableByteInteger()){
            value=json.getInteger("value");
        }
        if (value==null)
            throw new NullPointerException("value is null");
        return new MqttProperties(mqttProperty,value);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MqttProperties that = (MqttProperties) o;
        return property == that.property && Objects.equals(value, that.value);
    }

    @Override
    public int hashCode() {
        return Objects.hash(property, value);
    }

    @Override
    public String toString() {
        return "MqttProperties{" + "property=" + property + ", value=" + value + '}';
    }
}
