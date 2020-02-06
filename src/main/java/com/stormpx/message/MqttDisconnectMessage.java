package com.stormpx.message;

import com.stormpx.kit.StringPair;

import java.util.List;

public class MqttDisconnectMessage {
    private List<StringPair> userProperty;
    private String reasonString;

    public MqttDisconnectMessage(List<StringPair> userProperty, String reasonString) {
        this.userProperty = userProperty;
        this.reasonString = reasonString;
    }

    public List<StringPair> getUserProperty() {
        return userProperty;
    }

    public String getReasonString() {
        return reasonString;
    }
}
