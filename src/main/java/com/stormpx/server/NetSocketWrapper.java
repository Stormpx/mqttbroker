package com.stormpx.server;

import com.stormpx.mqtt.MqttDecoder;
import com.stormpx.mqtt.MqttEncoder;
import com.stormpx.mqtt.MqttSessionOption;
import com.stormpx.mqtt.packet.MqttPacket;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateHandler;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.impl.NetSocketInternal;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.NetSocket;
import io.vertx.core.net.SocketAddress;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class NetSocketWrapper implements MqttSocket {
    private final static Logger logger= LoggerFactory.getLogger(MqttSocket.class);

    private NetSocketInternal netSocketInternal;
    private ChannelHandlerContext ctx;
    private MqttSessionOption sessionOption;
    private Handler<MqttPacket> packetHandler;
    private Handler<Void> closeHandler;
    private boolean closed;

    public NetSocketWrapper(NetSocket netSocket,MqttSessionOption sessionOption) {
        this.netSocketInternal = (NetSocketInternal) netSocket;
        this.ctx=netSocketInternal.channelHandlerContext();
        this.sessionOption=sessionOption;
        initChannel(ctx,sessionOption);
        this.netSocketInternal.messageHandler(o->{
           if (o instanceof MqttPacket){
               Handler<MqttPacket> packetHandler = this.packetHandler;
               if (packetHandler!=null)
                   packetHandler.handle((MqttPacket) o);
           }
        });
        this.netSocketInternal.exceptionHandler(t->{
            if (!(t instanceof IOException)) {
                if (logger.isDebugEnabled()){
                    logger.debug("",t);
                }else{
                    logger.info("",t.getMessage());
                }
            }
        });
        this.netSocketInternal.closeHandler(v->{
            closed=true;
            Handler<Void> closeHandler = this.closeHandler;
            if (closeHandler!=null)
                closeHandler.handle(v);
        });
    }


    private void initChannel(ChannelHandlerContext ctx, MqttSessionOption sessionOption){
        ChannelPipeline pipeline = ctx.pipeline();
        pipeline.addBefore("handler","mqttEncoder",new MqttEncoder(sessionOption));
        pipeline.addBefore("handler","mqttDecoder",new MqttDecoder());
        pipeline.addBefore("handler","idleState",new IdleStateHandler(5,0,0));
        pipeline.addBefore("handler","readTimeoutHandler",new ChannelDuplexHandler(){
            @Override
            public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                if (evt== IdleState.READER_IDLE){
                    ctx.close();
                }else {
                    super.userEventTriggered(ctx, evt);
                }
            }
        });
    }


    @Override
    public MqttSessionOption getSessionOption() {
        return sessionOption;
    }

    @Override
    public SocketAddress remoteAddress() {
        return netSocketInternal.remoteAddress();
    }

    @Override
    public MqttSocket setKeepAlive(int keepAlive) {
        if (closed)
            return this;
        ChannelPipeline pipeline = ctx.pipeline();
        if (keepAlive==0){
            pipeline.remove("idleState");
        }else{
            pipeline.replace("idleState","idleState",new IdleStateHandler(keepAlive*1500,0,0, TimeUnit.MILLISECONDS));
        }
        return this;
    }


    @Override
    public MqttSocket pause() {
        netSocketInternal.pause();
        return this;
    }

    @Override
    public MqttSocket resume() {
        netSocketInternal.resume();
        return this;
    }

    @Override
    public MqttSocket handler(Handler<MqttPacket> handler) {
        this.packetHandler=handler;
        return this;
    }

    @Override
    public MqttSocket writePacket(MqttPacket packet) {
        return writePacket(packet,null);
    }

    @Override
    public MqttSocket writePacket(MqttPacket packet, Handler<AsyncResult<Void>> handler) {
//        if (!closed)
            netSocketInternal.writeMessage(packet,handler);
        return this;
    }

    @Override
    public MqttSocket closeHandler(Handler<Void> handler) {
        this.closeHandler=handler;
        return this;
    }

    @Override
    public MqttSocket close() {
        netSocketInternal.close();
        closed=true;
        return this;
    }

}
