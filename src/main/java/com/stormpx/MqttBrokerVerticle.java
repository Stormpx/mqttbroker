package com.stormpx;

import com.stormpx.auth.Authenticator;
import com.stormpx.broker.BrokerController;
import com.stormpx.broker.ClientController;
import com.stormpx.dispatcher.api.Session;
import com.stormpx.server.*;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class MqttBrokerVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger("mqttBroker");

    private MqttServer mqttServer;

    protected Authenticator authenticator;

    private BrokerController brokerController;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        this.brokerController=new BrokerController(vertx);

        onConfigRefresh(config());

        vertx.eventBus().<JsonObject>localConsumer(":config_change::",message->{
           onConfigRefresh(message.body());
        });



        initAuth()
                .compose(v->startServer())
                .compose(v->{
                    Integer verticleInstance = config().getInteger("verticle_instance", 0);
                    if (!config().getBoolean("repeat",false)&& verticleInstance -1 > 0){
                        Promise<String > promise=Promise.promise();
                        vertx.deployVerticle(MqttBrokerVerticle.class,new DeploymentOptions().setInstances(verticleInstance-1).setConfig(config().copy().put("repeat",true)),promise);
                        return promise.future();
                    }
                    return Future.succeededFuture();
                })
                .map((Void)null)
//                .onSuccess(v->logger.info("verticle:{} started",deploymentID()))
                .setHandler(startFuture);
    }


    protected void onConfigRefresh(JsonObject config) {
        brokerController.setMqttConfig(config.getJsonObject("mqtt",new JsonObject()));
    }


    protected Future<Void> initAuth(){
        ServiceLoader<Authenticator> authenticators = ServiceLoader.load(Authenticator.class);
        String auth = config().getString(Constants.AUTH, "anonymous");
        authenticators.forEach(authenticator ->{
            if (authenticator.name().equals(auth)){
                this.authenticator=authenticator;
            }
        } );
        if (this.authenticator!=null){
            if (!config().getBoolean("repeat",false))
                logger.info("init authenticator: {}",this.authenticator.name());
            return this.authenticator.init(vertx,config());
        }
        return Future.failedFuture("can't find authenticator :"+auth);
    }


    private Future<Void> startServer(){

        this.mqttServer = new MqttServerImpl(vertx,config().getBoolean("repeat",false)).setConfig(config());
        Session session=new Session(vertx);
        mqttServer
                .exceptionHandler(t->{
                    logger.error("{}",t.getMessage());
                    if (logger.isDebugEnabled())
                        logger.error("",t);
                })
                .handler(mqttContext-> new ClientController(vertx,mqttContext,brokerController,authenticator,session));

        return mqttServer.listen();
    }


    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        if (mqttServer!=null){
            mqttServer.close();
        }
        stopFuture.complete();
    }



}
