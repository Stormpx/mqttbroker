package com.stormpx.server;

public interface MqttConnectionHolder{


    void add(MqttContext mqttContext);

    MqttContext get(String clientId);

     void remove(String clientId);




//    void writeTo(String clientId, String topic, Buffer payload, MqttQoS qos);

//    void writeTo(String clientId, MqttBrokerMessage brokerMessage);


}
