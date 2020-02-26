package com.stormpx.server;

import com.stormpx.kit.StringPair;
import com.stormpx.mqtt.MqttVersion;
import com.stormpx.mqtt.ReasonCode;
import com.stormpx.message.*;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;

import java.util.List;
import java.util.function.Function;

public interface MqttContext {

    String id();

    MqttVersion version();

    boolean isCleanSession();

    boolean isRequestResponseInformation();

    boolean isClose();

    boolean isDisConnect();

    long sessionExpiryInterval();

    int keepAlive();

    int topicAliasMaximum();

    boolean isTakenOver();

    List<StringPair> userProperty();

    MqttSession session();

    MqttContext setSessionExpiryInterval(long sessionExpiryInterval);

    MqttContext setReceiveMaximum(int max);

    MqttContext setMaximumPacketSize(long max);

    MqttContext setClientIdentifier(String clientId);

    MqttContext setTopicAliasMaximum(int topicAliasMaximum);

    MqttContext setKeepAlive(int keepAlive);

    MqttContext setRetainAvailable(boolean retainAvailable);

    MqttContext setMaxQos(MqttQoS qos);

    MqttContext setWildcardSubscriptionAvailable(boolean subscriptionIdentifierAvailable);

    MqttContext setSubscriptionIdentifierAvailable(boolean subscriptionIdentifierAvailable);

    MqttContext setSharedSubscriptionAvailable(boolean sharedSubscriptionAvailable);

    MqttContext addUserProperty(StringPair stringPair);

    MqttContext addUserProperty(String key, String value);

    MqttContext setResponseInformation(String respInfo);

    MqttContext setAuthMethod(String authMethod);

    MqttContext setAuthData(Buffer data);

    MqttContext accept(boolean sessionPresent);

    MqttContext reject(byte code);


    MqttContext reject(boolean permanent, String serverReference);


    MqttContext takenOver(boolean sessionEnd);

    void handleException(Throwable throwable);

    /**
     *
     * @return messageId
     */
    Future<Integer> publish(MqttBrokerMessage message);

    void publishAcknowledge(int packetId, ReasonCode reasonCode, List<StringPair> userProperty, String reasonString);

    void publishReceived(int packetId,ReasonCode reasonCode,List<StringPair> userProperty,String reasonString);

    void publishRelease(int packetId,ReasonCode reasonCode,List<StringPair> userProperty,String reasonString);

    void publishComplete(int packetId,ReasonCode reasonCode,List<StringPair> userProperty,String reasonString);

    void subscribeAcknowledge(int packetId,List<ReasonCode> reasonCodes,List<StringPair> userProperty,String reasonString);

    void unSubscribeAcknowledge(int packetId,List<ReasonCode> reasonCodes,List<StringPair> userProperty,String reasonString);

    MqttContext exceptionHandler(Handler<Throwable> handler);

    MqttContext publishHandler(Handler<MqttPublishMessage> handler);

    MqttContext publishAcknowledgeHandler(Handler<Integer> handler);

    MqttContext publishReceiveHandler(Function<Integer,Future<Void>> handler);

    MqttContext publishReleaseHandler(Handler<Integer> handler);

    MqttContext publishCompleteHandler(Handler<Integer> handler);

    MqttContext subscribeHandler(Handler<MqttSubscribeMessage> handler);

    MqttContext unSubscribeHandler(Handler<MqttUnSubscribeMessage> handler);

    MqttContext disconnectHandler(Handler<MqttDisconnectMessage> handler);

    MqttContext closeHandler(Handler<Void> handler);

}
