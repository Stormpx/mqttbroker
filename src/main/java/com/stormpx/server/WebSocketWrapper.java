package com.stormpx.server;

import com.stormpx.mqtt.MqttDecoder;
import com.stormpx.mqtt.MqttEncoder;
import com.stormpx.mqtt.MqttSessionOption;
import com.stormpx.mqtt.packet.MqttPacket;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.TimeoutStream;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.net.SocketAddress;

import java.util.ArrayList;
import java.util.List;

public class WebSocketWrapper implements MqttSocket {
    private Vertx vertx;
    private WebSocketBase webSocketBase;
    private MqttSessionOption sessionOption;
    private boolean heartbeat=true;

    private List<Object> pending;
    private CompositeByteBuf buf;

    private MqttEncoder mqttEncoder;
    private MqttDecoder mqttDecoder;

    private TimeoutStream timeoutStream;
    private Handler<MqttPacket> packetHandler;
    private Handler<Void> closeHandler;

    public WebSocketWrapper(Vertx vertx,WebSocketBase webSocketBase, MqttSessionOption sessionOption) {
        this.vertx=vertx;
        this.webSocketBase = webSocketBase;
        this.sessionOption = sessionOption;
        this.mqttEncoder=new MqttEncoder(sessionOption);
        this.mqttDecoder=new MqttDecoder();
        this.buf=Unpooled.compositeBuffer();
        this.pending=new ArrayList<>();
        this.timeoutStream=vertx.periodicStream(5000).handler(this::handleTimeout);

        webSocketBase.frameHandler(frame->{
           if (!frame.isBinary()){
               close();
               return;
           }
           heartbeat=true;
            Buffer buffer = frame.binaryData();
            ByteBuf byteBuf = buffer.getByteBuf();

            buf.addComponent(true,byteBuf);

            while (byteBuf.isReadable()) {
                int readableBytes = buf.readableBytes();
                mqttDecoder.decode(byteBuf, pending);
                callHandler();
                if (readableBytes==buf.readableBytes()){
                    buf.discardReadComponents();
                    break;
                }
            }
        });
        webSocketBase.closeHandler(v->{
            if (timeoutStream!=null)
                timeoutStream.cancel();
            Handler<Void> closeHandler = this.closeHandler;
            if (closeHandler!=null)
                closeHandler.handle(v);
        });

    }

    private void callHandler(){
        if (!pending.isEmpty()){
            Handler<MqttPacket> packetHandler = this.packetHandler;
            if (packetHandler!=null) {
                for (Object o : pending) {
                    packetHandler.handle((MqttPacket) o);
                }
            }
            pending.clear();
        }
    }


    private void handleTimeout(long id){
        if (!heartbeat){
            //timeout
            close();
            vertx.cancelTimer(id);
            this.timeoutStream=null;
        }
        heartbeat=false;
    }

    @Override
    public MqttSessionOption getSessionOption() {
        return sessionOption;
    }

    @Override
    public SocketAddress remoteAddress() {
        return webSocketBase.remoteAddress();
    }

    @Override
    public MqttSocket setKeepAlive(int keepAlive) {
        if (timeoutStream!=null) {
            this.timeoutStream.cancel();
        }
        if (keepAlive!=0) {
            this.timeoutStream = vertx.periodicStream(keepAlive * 1500).handler(this::handleTimeout);
        }
        return this;
    }

    @Override
    public MqttSocket pause() {
        webSocketBase.pause();
        return this;
    }

    @Override
    public MqttSocket resume() {
        webSocketBase.resume();
        return this;
    }

    @Override
    public MqttSocket handler(Handler<MqttPacket> handler) {
        this.packetHandler=handler;
        return this;
    }

    @Override
    public MqttSocket writePacket(MqttPacket packet) {
        List<Object> list = new ArrayList<>(1);
        mqttEncoder.encode(packet,list);
        webSocketBase.writeBinaryMessage(Buffer.buffer((ByteBuf) list.get(0)));
        return this;
    }

    @Override
    public MqttSocket writePacket(MqttPacket packet, Handler<AsyncResult<Void>> handler) {
        List<Object> list = new ArrayList<>(1);
        mqttEncoder.encode(packet,list);
        webSocketBase.writeBinaryMessage(Buffer.buffer((ByteBuf) list.get(0)),handler);
        return this;
    }

    @Override
    public MqttSocket closeHandler(Handler<Void> handler) {
        this.closeHandler=handler;
        return this;
    }
    @Override
    public MqttSocket close() {
        if (!webSocketBase.isClosed()) {
            webSocketBase.close();
        }
        return this;
    }
}
