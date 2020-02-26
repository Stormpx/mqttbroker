package com.stormpx;

import com.stormpx.cluster.ClusterClient;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.MqttStateService;
import com.stormpx.cluster.StateService;
import com.stormpx.store.ClusterDataStore;
import com.stormpx.store.MessageStore;
import com.stormpx.store.SessionStore;
import com.stormpx.store.rocksdb.RocksDBClusterDataStore;
import com.stormpx.store.rocksdb.RocksDBMessageStore;
import com.stormpx.store.rocksdb.RocksDBSessionStore;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import org.rocksdb.RocksDBException;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterTest {
    public static void main(String[] args) throws RocksDBException {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        Vertx vertx=Vertx.vertx();
        String nodeId="";
        JsonObject json = new JsonObject();
        if (args[0].equals("56421"))
            nodeId="machine1";

        if (args[0].equals("56422"))
            nodeId="machine2";

        if (args[0].equals("56423"))
            nodeId="machine3";

        json.put("id",nodeId);
        json.put("port",Integer.valueOf(args[0]));
        JsonObject entries = new JsonObject();
        json.put("nodes",entries);
        if (!args[0].equals("56421"))
            entries.put("machine1","127.0.0.1:56421");

        if (!args[0].equals("56422"))
            entries.put("machine2","127.0.0.1:56422");

        if (!args[0].equals("56423"))
            entries.put("machine3","127.0.0.1:56423");

        String dir="D:\\rdb/"+nodeId;
        MessageStore messageStore=new RocksDBMessageStore(vertx,dir);
        SessionStore sessionStore=new RocksDBSessionStore(vertx,dir);
        ClusterDataStore clusterDataStore=new RocksDBClusterDataStore(vertx,dir,nodeId);

        MqttStateService stateService= new MqttStateService(vertx,messageStore,sessionStore);
        ClusterClient clusterClient=new ClusterClient(vertx, stateService,messageStore,clusterDataStore);
        new MqttCluster(vertx,json,clusterDataStore,stateService,clusterClient)
            .start()
                .onSuccess(v->{
                    System.out.println("start success");
                })
            .onFailure(t->{
                t.printStackTrace();
                vertx.close();
            });
    }
}
