package com.stormpx.dispatcher;

import com.stormpx.Constants;
import com.stormpx.cluster.mqtt.ClusterClient;
import com.stormpx.dispatcher.api.Cluster;
import com.stormpx.kit.*;
import com.stormpx.store.*;
import com.stormpx.store.rocksdb.Db;
import com.stormpx.store.rocksdb.RocksDBMessageStore;
import com.stormpx.store.rocksdb.RocksDBSessionStore;
import io.vertx.core.*;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;

public class DispatcherVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);

    private DispatcherController dispatcherController;

    @Override
    public void start(Future<Void> startFuture) throws Exception {


        JsonObject config = Optional.ofNullable(config()).orElse(new JsonObject());

        String saveDir = config.getString(Constants.SAVE_DIR);
        if (saveDir==null||saveDir.isBlank()){
            startFuture.tryFail("save dir is empty");
            return;
        }
        logger.info("save dir :{}",saveDir);
        Db.initialize(saveDir);
        SessionStore sessionStore= new RocksDBSessionStore(vertx);
        MessageStore messageStore=new RocksDBMessageStore(vertx);

        DispatcherContext dispatcherContext=new DispatcherContext(vertx,new TopicFilter());

        MessageService messageService=null;
        SessionService sessionService = null;
        SubscriptionService subscriptionService=null;

        boolean isCluster = config.getBoolean("isCluster",false);
        if (isCluster){
            JsonObject clusterConfig = config.getJsonObject("cluster");
            String id = clusterConfig.getString("id");
            Cluster cluster = new Cluster(vertx,id);

            messageService=new ClusterMessageService(messageStore,dispatcherContext,cluster);

            sessionService=new ClusterSessionService(sessionStore,dispatcherContext,cluster);
            subscriptionService=new ClusterSubscriptionServiceImpl(dispatcherContext,cluster);

        }else{
            messageService=new MessageService(messageStore,dispatcherContext);
            sessionService=new SessionService(sessionStore,dispatcherContext);
            subscriptionService=new SubscriptionServiceImpl(dispatcherContext);

        }

        dispatcherContext
                .setMessageService(messageService)
                .setSessionService(sessionService)
                .setSubscriptionService(subscriptionService)

        ;

        this.dispatcherController=new DispatcherController(dispatcherContext);

        startFuture.complete();


    }

    @Override
    public void stop(Future<Void> stopFuture) throws Exception {
        Db.destroy();
        stopFuture.complete();
    }
}
