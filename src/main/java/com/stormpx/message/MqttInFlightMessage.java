package com.stormpx.message;


import io.vertx.core.Promise;

public class MqttInFlightMessage {
    private boolean received;
    private Promise<Integer> promise;
    private MqttBrokerMessage message;

    private MqttInFlightMessage(MqttBrokerMessage message) {
        this.message = message;
        this.promise=Promise.promise();
    }



    public static MqttInFlightMessage wrapper(MqttBrokerMessage message){
        return new MqttInFlightMessage(message);
    }

    public boolean isReceived() {
        return received;
    }

    public MqttInFlightMessage setReceived(boolean received) {
        this.received = received;
        return this;
    }


    public Promise<Integer> getPromise() {
        return promise;
    }

    public MqttBrokerMessage getMessage() {
        return message;
    }
}
