package com.stormpx.cluster;

import com.stormpx.Constants;
import com.stormpx.cluster.message.RpcMessage;
import com.stormpx.cluster.mqtt.*;
import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.dispatcher.MessageContext;
import com.stormpx.dispatcher.api.Center;
import com.stormpx.kit.Codec;
import com.stormpx.kit.J;
import com.stormpx.kit.TopicUtil;
import com.stormpx.store.ClusterDataStore;
import com.stormpx.store.rocksdb.RocksDBClusterDataStore;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ClusterVerticle extends AbstractVerticle {
    private final static Logger logger= LoggerFactory.getLogger(ClusterClient.class);

    private ClusterDataStore clusterDataStore;

    private MqttCluster mqttCluster;
    private MqttStateService stateService;
    private ClusterClient clusterClient;

    private Center center;

    @Override
    public void start(Future<Void> startFuture) throws Exception {
        JsonObject config = Optional.ofNullable(config()).orElse(new JsonObject());
        String saveDir = config.getString(Constants.SAVE_DIR);
        if (saveDir==null||saveDir.isBlank()){
            startFuture.tryFail("save dir is empty");
            return;
        }
        JsonObject cluster = config.getJsonObject("cluster");
        if (cluster==null)
            cluster=new JsonObject();

        String id = cluster.getString("id");
        Integer port = cluster.getInteger("port");
        JsonObject nodes = cluster.getJsonObject("nodes");
        cluster.put(Constants.SAVE_DIR,saveDir);
        //cluster enable
        this.clusterDataStore=new RocksDBClusterDataStore(vertx,id);
        this.stateService=new MqttStateService(vertx);
        this.center =new Center(vertx);

        //            this.stateService.addHandler("/store/sync",this::);
        //            this.stateService.addHandler("/session/sync",);
        this.stateService.addHandler("/session",json->{
            String clientId = json.getString("clientId");
            return center.getSession(clientId);
        });
        this.stateService.addHandler("/message",json->{
            String mid = json.getString("id");
            return center.getMessage(mid);
        });
        this.stateService.addHandler("/takenover",center::takenOverSession);

        this.stateService.addHandler("/dispatcher",json->{
            JsonArray share = json.getJsonArray("share", J.EMPTY_ARRAY);
            byte[] binary = json.getBinary("message");
            DispatcherMessage dispatcherMessage = Codec.decodeDispatcherMessage(Buffer.buffer(binary));
            MessageContext messageContext = new MessageContext(dispatcherMessage);
            messageContext.setShareTopics(share.stream().map(Object::toString).collect(Collectors.toSet()));
            return center.dispatcher(messageContext);
        });

        this.stateService.reSetSessionHandler(center::resetSession);

        this.clusterClient=new ClusterClient(vertx,stateService,clusterDataStore);
        this.mqttCluster=new MqttCluster(vertx,cluster,clusterDataStore,stateService,clusterClient);
        logger.info("cluster enable id:{} port:{} nodes:{}",id,port,nodes);



        new ClusterApiController(vertx,clusterClient,stateService,mqttCluster);



        mqttCluster.start().setHandler(startFuture);
    }

    @Override
    public void stop(Promise<Void> stopPromise) throws Exception {

    }
}
