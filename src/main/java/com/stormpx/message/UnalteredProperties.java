package com.stormpx.message;

import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttProperties;
import com.stormpx.mqtt.MqttProperty;
import io.vertx.core.buffer.Buffer;

import java.util.ArrayList;
import java.util.List;

public class UnalteredProperties {

    private List<MqttProperties> unalteredProperties;
    private boolean payloadFormatIndicator;
    private String responseTopic;
    private Buffer correlationData;
    private List<StringPair> userProperty;
    private String contentType;

    public UnalteredProperties() {
        this.unalteredProperties=new ArrayList<>();
    }

    public UnalteredProperties(List<MqttProperties> unalteredProperties) {
        this.unalteredProperties = unalteredProperties;
    }

    public List<MqttProperties> getUnalteredProperties() {
        return unalteredProperties;
    }



    public UnalteredProperties setPayloadFormatIndicator(boolean payloadFormatIndicator) {
        this.payloadFormatIndicator=payloadFormatIndicator;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.PAYLOAD_FORMAT_INDICATOR,(byte)(payloadFormatIndicator?1:0)));
        return this;
    }
    public UnalteredProperties setResponseTopic(String responseTopic) {
        this.responseTopic = responseTopic;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.RESPONSE_TOPIC,responseTopic));
        return this;
    }

    public UnalteredProperties setCorrelationData(Buffer correlationData) {
        this.correlationData = correlationData;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.CORRELATION_DATA,correlationData.getByteBuf()));
        return this;
    }


    public UnalteredProperties setContentType(String contentType) {
        this.contentType=contentType;
        this.unalteredProperties.add(new MqttProperties(MqttProperty.CONTENT_TYPE,contentType));
        return this;
    }

    public UnalteredProperties addUserProperty(StringPair stringPair){
        this.unalteredProperties.add(new MqttProperties(MqttProperty.USER_PROPERTY,stringPair));
        if (this.userProperty==null){
            this.userProperty=new ArrayList<>();
        }
        this.userProperty.add(stringPair);
        return this;
    }

    public boolean isPayloadFormatIndicator() {
        return payloadFormatIndicator;
    }

    public String getResponseTopic() {
        return responseTopic;
    }

    public Buffer getCorrelationData() {
        return correlationData;
    }

    public List<StringPair> getUserProperty() {
        return userProperty;
    }

    public String getContentType() {
        return contentType;
    }
}
