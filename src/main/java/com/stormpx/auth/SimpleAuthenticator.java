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

public class SimpleAuthenticator implements Authenticator {
    private AuthResult<Boolean> authResult=AuthResult.create(true);
    @Override
    public String name() {
        return "anonymous";
    }

    @Override
    public Future<Void> init(Vertx vertx, JsonObject config) {
        return Future.succeededFuture();
    }

    @Override
    public Future<AuthResult<Boolean>> authorize(String clientId, MqttAuth auth,List<StringPair> userProperty) {
        return Future.succeededFuture(authResult);
    }

    @Override
    public Future<AuthResult<Boolean>> authorizePub(String clientId, String topic) {
        return Future.succeededFuture(authResult);
    }

    @Override
    public Future<AuthResult<List<ReasonCode>>> authorizeSub(String clientId, List<MqttSubscription> mqttSubscriptions,List<StringPair> userProperty) {
        return Future.succeededFuture(AuthResult.create(mqttSubscriptions.stream().map(MqttSubscription::getQos).map(qos->ReasonCode.valueOf((byte) qos.value())).collect(Collectors.toList())));
    }
}
