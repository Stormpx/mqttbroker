package com.stormpx.server;

import com.stormpx.Constants;
import com.stormpx.kit.J;
import com.stormpx.mqtt.ControlPacketType;
import com.stormpx.mqtt.MqttSessionOption;
import com.stormpx.mqtt.MqttVersion;
import com.stormpx.mqtt.packet.MqttConnectPacket;
import com.stormpx.mqtt.packet.MqttInvalidPacket;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.http.WebSocketBase;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.*;
import io.vertx.core.shareddata.LocalMap;

import java.util.HashSet;
import java.util.Set;

import static com.stormpx.Constants.*;
import static com.stormpx.Constants.SSL;

public class MqttServerImpl implements MqttServer {
    private final static Logger logger = LoggerFactory.getLogger(MqttServer.class);
    private final static Set<Integer> set=new HashSet<>();

    private Vertx vertx;
    private MqttServerOption mqttServerOption;
    private NetServer netServer;
    private HttpServer httpServer;
    private Handler<Throwable>  exceptionHandler;
    private Handler<MqttContext> handler;

    public MqttServerImpl(Vertx vertx) {
        this.vertx =vertx;
    }


    @Override
    public MqttServer exceptionHandler(Handler<Throwable> handler) {
        this.exceptionHandler=handler;
        return this;
    }

    @Override
    public MqttServer handler(Handler<MqttContext> handler) {
        this.handler=handler;
        return this;
    }

    @Override
    public MqttServer setConfig(JsonObject config) {
        MqttServerOption mqttServerOption = new MqttServerOption();

        mqttServerOption.setTcpNoDelay(config.getBoolean(Constants.TCP_NO_DELAY,false));
        mqttServerOption.setSni(config.getBoolean(Constants.SNI,false));


        JsonObject tcpJson = config.getJsonObject(TCP, new JsonObject());
        boolean tcp=tcpJson.getBoolean(Constants.ENABLE,true);
        if (tcp){
            JsonArray jsonArray = tcpJson.getJsonArray(KEY_CERT);
            if (jsonArray!=null){
                PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions();
                J.toJsonStream(jsonArray)
                        .forEach(json->{
                            String keyPath = json.getString(KEY_FILE);
                            String certPath = json.getString(CERT_FILE);
                            if (keyPath!=null&&certPath!=null){
                                //log control
                                LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap("storm");
                                localMap.computeIfAbsent("tcp"+keyPath+certPath,k->{
                                    logger.info("mqtt tcp key_path:{} cert_path:{}",keyPath,certPath);
                                    return keyPath+certPath;
                                });

                                pemKeyCertOptions.addKeyPath(keyPath);
                                pemKeyCertOptions.addCertPath(certPath);
                            }
                        });
                mqttServerOption.setTcpPemKeyCertOptions(pemKeyCertOptions);
            }
            mqttServerOption.setTcpSsl(tcpJson.getBoolean(SSL,false));
            mqttServerOption.setTcpHort(tcpJson.getString(Constants.HOST,"0.0.0.0"));
            mqttServerOption.setTcpPort(tcpJson.getInteger(Constants.PORT, mqttServerOption.isTcpSsl()?8883:1883));
        }

        JsonObject wsJson = config.getJsonObject(WS, new JsonObject());
        Boolean ws = wsJson.getBoolean(Constants.ENABLE, false);
        if (ws){
            JsonArray jsonArray = wsJson.getJsonArray(KEY_CERT);
            if (jsonArray!=null){
                PemKeyCertOptions pemKeyCertOptions = new PemKeyCertOptions();
                J.toJsonStream(jsonArray)
                        .forEach(json->{
                            String keyPath = json.getString(KEY_FILE);
                            String certPath = json.getString(CERT_FILE);
                            if (keyPath!=null&&certPath!=null){
                                //log control
                                LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap("storm");
                                localMap.computeIfAbsent("ws"+keyPath+certPath,k->{
                                    logger.info("mqtt ws key_path:{} cert_path:{}",keyPath,certPath);
                                    return keyPath+certPath;
                                });
                                pemKeyCertOptions.addKeyPath(keyPath);
                                pemKeyCertOptions.addCertPath(certPath);
                            }
                        });
                mqttServerOption.setWsPemKeyCertOptions(pemKeyCertOptions);
            }
            mqttServerOption.setWsSsl(wsJson.getBoolean(SSL,false));
            mqttServerOption.setWsHort(wsJson.getString(Constants.HOST,"0.0.0.0"));
            mqttServerOption.setWsPort(wsJson.getInteger(Constants.PORT, mqttServerOption.isTcpSsl()?8883:1883));
            mqttServerOption.setWsPath(wsJson.getString(Constants.PATH,"/mqtt"));
        }
        this.mqttServerOption=mqttServerOption;

        return this;
    }


    @Override
    public Future<Void> listen(){
        Future<Void> future=Future.succeededFuture();
        if (this.mqttServerOption.isTcpEnable()){
            future=tcpListen();
        }
        if (this.mqttServerOption.isWsEnable()){
            future=future.compose(v->wsListen());
        }
        return future;
    }

    public Future<Void> tcpListen() {
        Promise<Void> promise=Promise.promise();
        NetServerOptions netServerOptions = new NetServerOptions();
        netServerOptions.setSni(mqttServerOption.isSni());
        netServerOptions.setTcpNoDelay(mqttServerOption.isTcpNoDelay());
        netServerOptions.setSsl(mqttServerOption.isTcpSsl());
        if (mqttServerOption.getTcpPemKeyCertOptions()!=null)
            netServerOptions.setPemKeyCertOptions(mqttServerOption.getTcpPemKeyCertOptions());

       /* TCPSSLOptions tcl = netServerOptions;


        tcl
                .setPfxTrustOptions(new PfxOptions().setPath())
                .setPfxKeyCertOptions(new PfxOptions().setPath().setPassword())

                .setTrustStoreOptions(new JksOptions().setPath(""))
                .setKeyStoreOptions(new JksOptions().setPath().setPassword())

                .setPemTrustOptions(new PemTrustOptions().addCertPath(""))
                .setPemKeyCertOptions(new PemKeyCertOptions().addCertPath().addKeyPath())

                .setJdkSslEngineOptions(new JdkSSLEngineOptions())
                .setOpenSslEngineOptions(new OpenSSLEngineOptions())*/


        netServerOptions.setHost(mqttServerOption.getTcpHort());
        netServerOptions.setPort(mqttServerOption.getTcpPort());


        this.netServer= vertx.createNetServer(netServerOptions.setUsePooledBuffers(false));

        this.netServer.connectHandler(netSocket-> handleNetSocket(netSocket,handler,exceptionHandler));
        this.netServer.listen(ar->{
            if (ar.succeeded()){
                LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap("storm");
                localMap.computeIfAbsent(netServerOptions.getPort(),(k)->{
                    logger.info("start tcp server success port:{} ssl:{}", netServerOptions.getPort(),netServerOptions.isSsl());
                    return netServerOptions.getPort();
                });
                promise.complete();
            }else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    public Future<Void> wsListen() {
         if (this.httpServer!=null)
             return Future.succeededFuture();

        Promise<Void> promise=Promise.promise();
        String wsPath = mqttServerOption.getWsPath();
        HttpServerOptions httpServerOptions = new HttpServerOptions();
        httpServerOptions.setWebsocketSubProtocols("mqtt,mqttv3.1");
        httpServerOptions.setSni(mqttServerOption.isSni());
        httpServerOptions.setTcpNoDelay(mqttServerOption.isTcpNoDelay());
        httpServerOptions.setSsl(mqttServerOption.isWsSsl());
        if (mqttServerOption.getWsPemKeyCertOptions()!=null)
            httpServerOptions.setPemKeyCertOptions(mqttServerOption.getWsPemKeyCertOptions());
        httpServerOptions.setHost(mqttServerOption.getWsHort());
        httpServerOptions.setPort(mqttServerOption.getWsPort());

        this.httpServer= vertx.createHttpServer(httpServerOptions);
        this.httpServer.exceptionHandler(Throwable::printStackTrace);
        this.httpServer.websocketHandler(websocket->{
             handleWebSocket(websocket,wsPath,handler,exceptionHandler);
        });
         this.httpServer.listen(ar->{
             if (ar.succeeded()){
                 LocalMap<Object, Object> localMap = vertx.sharedData().getLocalMap("storm");
                 localMap.computeIfAbsent(httpServerOptions.getPort(),(k)->{

                     logger.info("start http server success port:{} path:{} ssl:{}",httpServerOptions.getPort(),wsPath,httpServerOptions.isSsl());
                     return httpServerOptions.getPort();
                 });
                 promise.complete();
             }else {
                 promise.fail(ar.cause());
             }
         });

        return promise.future();
    }


    @Override
    public Future<Void> close() {
        return closeNetServer()
                .onComplete(v->closeHttpServer());
    }

    private Future<Void> closeNetServer(){
        if (netServer!=null){
            Promise<Void> promise = Promise.promise();
            netServer.close(promise);
            return promise.future();
        }

        return Future.succeededFuture();
    }

    private Future<Void> closeHttpServer(){
        if (httpServer!=null){
            Promise<Void> promise = Promise.promise();
            httpServer.close(promise);
            return promise.future();
        }

        return Future.succeededFuture();
    }


    void handleNetSocket(NetSocket netSocket, Handler<MqttContext> handler, Handler<Throwable> exceptionHandler){
        MqttSessionOption mqttSessionOption = new MqttSessionOption();
        MqttSocket mqttSocket = MqttSocket.wrapper(netSocket, mqttSessionOption);
        handleNewConnect(mqttSocket,handler,exceptionHandler);

    }
    void handleWebSocket(ServerWebSocket webSocketBase,String wsPath, Handler<MqttContext> handler, Handler<Throwable> exceptionHandler){
        if (!webSocketBase.path().equals(wsPath)) {
            webSocketBase.reject();
            return;
        }
        webSocketBase.accept();

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
