package com.stormpx.broker;

import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttProperty;
import com.stormpx.kit.J;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.Iterator;
import java.util.List;

public class MqttWill {
    private boolean willFlag;
    private List<MqttProperties> willProperties;
    private boolean retain;
    private MqttQoS qos;
    private String willTopic;
    private Buffer willPayload;
    private Long willDelayInterval=0L;
    private Long messageExpiryInterval=0xffffffffL;

    public MqttWill(boolean willFlag,List<MqttProperties> willProperties, boolean retain, MqttQoS qoS, String willTopic, Buffer willPayload) {
        this.willFlag=willFlag;
        this.willProperties = willProperties;
        this.retain = retain;
        this.qos = qoS;
        this.willTopic = willTopic;
        this.willPayload = willPayload;
        if (willProperties!=null) {
            for (Iterator<MqttProperties> iterator = willProperties.iterator(); iterator.hasNext(); ) {
                MqttProperties willProperty = iterator.next();
                if (willProperty.getProperty() == MqttProperty.WILL_DELAY_INTERVAL) {
                    this.willDelayInterval = (Long) willProperty.getValue();
                    iterator.remove();
                } else if (willProperty.getProperty() == MqttProperty.MESSAGE_EXPIRY_INTERVAL) {
                    this.messageExpiryInterval = (Long) willProperty.getValue();
                    iterator.remove();
                }
            }
        }
    }

    public JsonObject toJson(){
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("topic",willTopic)
                .put("qos",qos.value())
                .put("retain",retain)
                .put("payload",willPayload.getBytes())
                .put("delayInterval",willDelayInterval)
                .put("messageExpiryInterval",messageExpiryInterval);

        if (willProperties!=null) {
            JsonArray array = willProperties.stream()
                    .map(MqttProperties::toJson)
                    .collect(J.toJsonArray());

            jsonObject.put("properties", array);
        }
        return jsonObject;
    }


    public boolean isWillFlag(){
        return willFlag;
    }

    public List<MqttProperties> getWillProperties() {
        return willProperties;
    }

    public boolean isRetain() {
        return retain;
    }

    public MqttQoS getQos() {
        return qos;
    }

    public String getWillTopic() {
        return willTopic;
    }

    public Buffer getWillPayload() {
        return willPayload;
    }

    public Long getWillDelayInterval() {
        return willDelayInterval;
    }

    public Long getMessageExpiryInterval() {
        return messageExpiryInterval;
    }

}
