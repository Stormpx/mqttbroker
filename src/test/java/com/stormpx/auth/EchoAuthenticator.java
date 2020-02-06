package com.stormpx.auth;

import com.stormpx.message.MqttAuth;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.ReasonCode;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.stream.Collectors;

public class EchoAuthenticator implements Authenticator {
    @Override
    public String name() {
        return "echo";
    }

    @Override
    public Future<Void> init(Vertx vertx, JsonObject config) {
        System.out.println("echo init...");
        return Future.succeededFuture();
    }

    @Override
    public Future<AuthResult<Boolean>> authorize(String clientId, MqttAuth auth,List<StringPair> userProperty) {
        return Future.succeededFuture(AuthResult.create(true).setPairList(userProperty));
    }

    @Override
    public Future<AuthResult<Boolean>> authorizePub(String clientId, String topic) {
        return Future.succeededFuture(AuthResult.create(true));
    }

    @Override
    public Future<AuthResult<List<ReasonCode>>> authorizeSub(String clientId, List<MqttSubscription> mqttSubscriptions, List<StringPair> userProperty) {
        System.out.println(userProperty.size());
        return Future.succeededFuture(AuthResult.create(mqttSubscriptions.stream().map(MqttSubscription::getQos).map(qos->ReasonCode.valueOf((byte) qos.value())).collect(Collectors.toList())).setPairList(userProperty));
    }
}
