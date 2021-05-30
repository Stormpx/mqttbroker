package com.stormpx.broker;

import com.stormpx.kit.TopicFilter;
import com.stormpx.kit.TopicUtil;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.ReasonCode;
import io.netty.handler.codec.mqtt.MqttQoS;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.ListIterator;

public class Subscribe {
    private final static Logger logger= LoggerFactory.getLogger(Subscribe.class);
    private String clientId;
    private List<MqttSubscription> subscriptions;

    public Subscribe(String clientId,List<MqttSubscription> subscriptions) {
        this.clientId=clientId;
        this.subscriptions = subscriptions;
    }

    public Subscribe filter(List<ReasonCode> reasonCodes){

        ListIterator<MqttSubscription> subscriptionListIterator = subscriptions.listIterator();
        ListIterator<ReasonCode> reasonCodeListIterator = reasonCodes.listIterator();
        while (subscriptionListIterator.hasNext()){
            MqttSubscription mqttSubscription = subscriptionListIterator.next();
            ReasonCode reasonCode = reasonCodeListIterator.next();
            if (reasonCode.byteValue() >= 0x03) {
                logger.debug("client:{} subscribe topic:{} qos:{} fail reason:{}",clientId, mqttSubscription.getTopicFilter(), mqttSubscription.getQos(), reasonCode.name().toLowerCase());
                subscriptionListIterator.remove();
                continue;
            }
            MqttQoS mqttQoS = MqttQoS.valueOf(reasonCode.byteValue());
            mqttSubscription.setQos(mqttQoS);
            logger.debug("client:{} subscribe topic:{} qos:{} success", clientId, mqttSubscription.getTopicFilter(), mqttSubscription.getQos());

        }
        return this;

    }


    /**
     * get matched topic maximum qos
     * @param topic
     * @return
     */
    public MqttSubscription getMaxMatchSubscription(String topic){
        return subscriptions.stream()
                .filter(mqttSubscription -> TopicUtil.matches(mqttSubscription.getTopicFilter(), topic))
                .max(Comparator.comparingInt(m -> m.getQos().value()))
                .orElse(null);
    }


    public List<MqttSubscription> getSubscriptions(){
        return this.subscriptions;
    }
}
