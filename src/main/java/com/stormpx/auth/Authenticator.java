package com.stormpx.auth;

import com.stormpx.message.MqttAuth;
import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.ReasonCode;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface Authenticator {

    String name();

    Future<Void> init(Vertx vertx,JsonObject config);

    Future<AuthResult<Boolean>> authorize(String clientId,MqttAuth auth,List<StringPair> userProperty);

    Future<AuthResult<Boolean>> authorizePub(String clientId,String topic);

    Future<AuthResult<List<ReasonCode>>> authorizeSub(String clientId, List<MqttSubscription> mqttSubscriptions, List<StringPair> userProperty);


}
