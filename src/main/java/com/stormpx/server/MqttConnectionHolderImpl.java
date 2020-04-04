package com.stormpx.server;

import com.stormpx.mqtt.ControlPacketType;
import com.stormpx.mqtt.MqttSessionOption;
import com.stormpx.mqtt.MqttVersion;
import com.stormpx.mqtt.packet.MqttConnectPacket;
import com.stormpx.mqtt.packet.MqttInvalidPacket;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;

import java.util.HashMap;
import java.util.Map;

public class MqttConnectionHolderImpl implements MqttConnectionHolder {
    private final static Logger logger= LoggerFactory.getLogger(MqttConnectionHolder.class);
    private Vertx vertx;
    private Map<String, MqttContext> connections;

    public MqttConnectionHolderImpl(Vertx vertx) {
        this.connections=new HashMap<>();
        this.vertx = vertx;
    }


    @Override
    public void add(MqttContext mqttContext) {
        connections.put(mqttContext.session().clientIdentifier(), mqttContext);
    }

    @Override
    public MqttContext get(String clientId) {
        return connections.get(clientId);
    }

    @Override
    public void remove(String clientId) {
        connections.remove(clientId);
    }



    void handleNetSocket(NetSocket netSocket, Handler<MqttContext> handler, Handler<Throwable> exceptionHandler){
        MqttSessionOption mqttSessionOption = new MqttSessionOption();
        MqttSocket mqttSocket = MqttSocket.wrapper(netSocket, mqttSessionOption);
        handleNewConnect(mqttSocket,handler,exceptionHandler);

    }
    void handleWebSocket(WebSocketBase webSocketBase, Handler<MqttContext> handler, Handler<Throwable> exceptionHandler){
        MqttSessionOption mqttSessionOption = new MqttSessionOption();
        MqttSocket mqttSocket = MqttSocket.wrapper(vertx,webSocketBase, mqttSessionOption);
        handleNewConnect(mqttSocket,handler,exceptionHandler);

    }

     private void handleNewConnect(MqttSocket mqttSocket, Handler<MqttContext> handler, Handler<Throwable> exceptionHandler) {
        var ref = new Object() {
            AbstractMqttContext mqttConnection;
        };
        mqttSocket.handler(packet->{
            try {
                if (packet != null) {
                    if (ref.mqttConnection==null) {
                        if (packet instanceof MqttInvalidPacket){
                            Throwable cause = packet.cause();
                            logger.info("receive invalid packet cause:{} ...",cause.getMessage());
                            mqttSocket.close();
                            return;
                        }

                        ControlPacketType packetType = packet.fixedHeader().getPacketType();
                        if (packetType != ControlPacketType.CONNECT || !(packet instanceof MqttConnectPacket))
                            throw new RuntimeException("except handle CONNECT packet");

                        MqttConnectPacket connectPacket = (MqttConnectPacket) packet;

                        MqttVersion version = connectPacket.getVersion();
                        if (version == MqttVersion.MQTT_5_0) {
                            ref.mqttConnection = new Mqtt5Context(mqttSocket,connectPacket);
                        } else {
                            ref.mqttConnection=new Mqtt3Context(mqttSocket,connectPacket);

                        }
                        if (handler!=null){
                            handler.handle(ref.mqttConnection);
                        }
                    }else{
                        ref.mqttConnection.handle(packet);
                    }
                }
            } catch (RuntimeException e) {
                if (exceptionHandler!=null) {
                    exceptionHandler.handle(e);
                }
                mqttSocket.close();
            }
        });


    }


}
