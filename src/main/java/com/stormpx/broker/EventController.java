package com.stormpx.broker;

import com.stormpx.dispatcher.EventContext;
import com.stormpx.dispatcher.MessageContext;
import com.stormpx.dispatcher.PacketIds;
import com.stormpx.mqtt.MqttSubscription;
import com.stormpx.mqtt.ReasonCode;
import com.stormpx.server.MqttContext;
import io.vertx.core.Vertx;
import io.vertx.core.eventbus.MessageConsumer;

import java.util.List;

public class EventController {
    private Vertx vertx;
    private MqttContext mqttContext;
    private BrokerController brokerController;
    private ClientController clientController;
    private MessageConsumer<EventContext> messageConsumer;

    public EventController(Vertx vertx, MqttContext mqttContext, BrokerController brokerController, ClientController clientController) {
        this.vertx = vertx;
        this.mqttContext = mqttContext;
        this.brokerController = brokerController;
        this.clientController = clientController;
        init();
    }

    private void init(){
        String clientId = mqttContext.session().clientIdentifier();
        this.messageConsumer =vertx.eventBus().<EventContext>localConsumer("_"+clientId+"_"+mqttContext.id()+"message_")
                .handler(message->{
                    try {
                        EventContext eventContext = message.body();
                        if (eventContext.isPacketId()){
                            handlePacketIds(eventContext.asPacketIds());
                        }else if (eventContext.isMessage()){
                            handleMessage(eventContext.asMessage());
                        }else if (eventContext.isSubscribe()){
                            handleSubscribe(eventContext.asSubscribeList());
                        }
                    } catch (Exception e) {
                        mqttContext.handleException(e);
                    }
                });
    }

    private void handlePacketIds(PacketIds packetIds){
        if (packetIds.isUnReceived()){
            packetIds.getId().forEach(packetId->mqttContext.publishRelease(packetId, ReasonCode.SUCCESS,null,null));
        }else if (packetIds.isUnConfirm()){
            packetIds.getId().forEach(mqttContext.session()::addPacketId);
        }
    }

    private void handleMessage(MessageContext context){
        if (context.getPacketId()==null){
            clientController.send(context.getMessage(),context.getSubscriptionIds());
        }else{
            clientController.resend(context.getPacketId(),context.getMessage(),context.getSubscriptionIds());
        }
    }

    private void handleSubscribe(List<MqttSubscription> mqttSubscriptions){
        brokerController.reSubscribe(mqttContext.session().clientIdentifier(),mqttSubscriptions);
    }

    public void cancel(){
        messageConsumer.unregister();
    }


    public MessageConsumer<EventContext> getMessageConsumer() {
        return messageConsumer;
    }
}
