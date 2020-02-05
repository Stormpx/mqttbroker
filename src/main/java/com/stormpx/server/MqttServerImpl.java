package com.stormpx.server;

import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.core.net.*;

public class MqttServerImpl implements MqttServer {
    private final static Logger logger = LoggerFactory.getLogger(MqttServer.class);
    private Vertx vertx;
    private MqttServerOption mqttServerOption;
    private NetServer netServer;
    private HttpServer httpServer;
    private MqttConnectionHolderImpl mqttConnectionHolder;
    private Handler<Throwable>  exceptionHandler;
    private Handler<MqttContext> handler;

    public MqttServerImpl(Vertx vertx,MqttServerOption mqttServerOption) {
        this.vertx =vertx;
        this.mqttServerOption = mqttServerOption;
        this.mqttConnectionHolder=new MqttConnectionHolderImpl(vertx);
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
    public MqttConnectionHolder holder() {
        return mqttConnectionHolder;
    }


    @Override
    public Future<Void> listen() {
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

        this.netServer.connectHandler(netSocket->{
            this.mqttConnectionHolder.handleWebSocket(netSocket,handler,exceptionHandler);
        });
        this.netServer.listen(ar->{
            if (ar.succeeded()){
                logger.info("start tcp server success port:{}",netServerOptions.getPort());
                promise.complete();
            }else {
                promise.fail(ar.cause());
            }
        });

        return promise.future();
    }

    @Override
    public Future<Void> wsListen() {
         if (this.httpServer!=null)
             return Future.succeededFuture();

        Promise<Void> promise=Promise.promise();
        String wsPath = mqttServerOption.getWsPath();
        HttpServerOptions httpServerOptions = new HttpServerOptions();
        httpServerOptions.setWebsocketSubProtocols("mqtt");
        httpServerOptions.setSni(mqttServerOption.isSni());
        httpServerOptions.setTcpNoDelay(mqttServerOption.isTcpNoDelay());
        httpServerOptions.setSsl(mqttServerOption.isWsSsl());
        if (mqttServerOption.getWsPemKeyCertOptions()!=null)
            httpServerOptions.setPemKeyCertOptions(mqttServerOption.getWsPemKeyCertOptions());
        httpServerOptions.setHost(mqttServerOption.getWsHort());
        httpServerOptions.setPort(mqttServerOption.getWsPort());

        this.httpServer= vertx.createHttpServer(httpServerOptions);

         this.httpServer.websocketHandler(websocket->{
             if (!websocket.path().equals(wsPath)) {
                 websocket.reject();
                 return;
             }
             websocket.accept();
             this.mqttConnectionHolder.handleWebSocket(websocket,handler,exceptionHandler);
         });

         this.httpServer.listen(ar->{
             if (ar.succeeded()){
                 logger.info("start http server success port:{} path:{}",httpServerOptions.getPort(),wsPath);
                 promise.complete();
             }else {
                 promise.fail(ar.cause());
             }
         });

        return promise.future();
    }


    @Override
    public Future<Void> close() {
        return closeNetServer().compose(v->closeHttpServer());
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
}
