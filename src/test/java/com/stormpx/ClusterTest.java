package com.stormpx;

import com.stormpx.cluster.*;
import com.stormpx.cluster.mqtt.ClusterClient;
import com.stormpx.cluster.MqttCluster;
import com.stormpx.cluster.mqtt.MqttStateService;
import com.stormpx.store.ClusterDataStore;
import com.stormpx.store.MessageStore;
import com.stormpx.store.SessionStore;
import com.stormpx.store.memory.MemoryClusterDataStore;
import com.stormpx.store.rocksdb.RocksDBClusterDataStore;
import com.stormpx.store.rocksdb.RocksDBMessageStore;
import com.stormpx.store.rocksdb.RocksDBSessionStore;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.rocksdb.RocksDBException;

@ExtendWith(VertxExtension.class)
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

        MqttStateService stateService= new MqttStateService(vertx);
        ClusterClient clusterClient=new ClusterClient(vertx, stateService,clusterDataStore);
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




    @Test
    public void logTest(){
        LogList logList = new LogList(new MemoryClusterDataStore(),0,0);
        //add
        Buffer test = Buffer.buffer("test");
        logList.addLog("asd",1,1,1,test);
        logList.addLog("asd",1,2,1,test);
        logList.addLog("asd",1,3,1,test);
        logList.addLog("asd",1,4,1,test);
        logList.addLog("asd",1,5,1,test);
        logList.addLog("asd",1,6,1,test);
        logList.addLog("asd",1,7,1,test);
        logList.addLog("asd",1,8,1,test);
        logList.addLog("asd",1,9,1,test);
        logList.addLog("asd",1,10,1,test);


        logList.releasePrefix(3);

        logList.getLog(4)
            .onSuccess(log->{
                Assertions.assertEquals(log.getProposalId(),4);
            });

        logList.getLog(7)
                .onSuccess(log->{
                    Assertions.assertEquals(log.getProposalId(),7);
                });

        logList.getLog(1,9)
                .onSuccess(log->{
                    Assertions.assertEquals(8,log.size());
                    int i=1;
                    for (LogEntry logEntry : log) {
                        Assertions.assertEquals(logEntry.getProposalId(),i++);
                    }
                    System.out.println("21321");
                });


        logList.releasePrefix(3);

        logList.getLog(1)
                .onSuccess(log->{

                    Assertions.assertEquals(log.getProposalId(),1);
                });

        logList.releasePrefix(10);

        logList.getLog(10)
                .onSuccess(log->{

                    Assertions.assertEquals(log.getProposalId(),10);
                });


        logList.setLog(new LogEntry().setNodeId("123").setIndex(5).setTerm(2).setProposalId(15),true);

        logList.getLog(5)
                .onSuccess(log->{
                    Assertions.assertEquals(log.getProposalId(),15);
                });

        logList.addLog("asdw",3,11,1,test);

        Assertions.assertEquals(logList.getLastLogIndex(),11);

        logList.getLastLog()
                .onSuccess(log->{
                    Assertions.assertEquals(log.getProposalId(),11);
                    Assertions.assertEquals(log.getTerm(),3);
                });

        logList.truncateSuffix(8);

        Assertions.assertEquals(logList.getLastLogIndex(),7);

        logList.getLastLog()
                .onSuccess(log->{
                    Assertions.assertEquals(log.getProposalId(),7);
                    Assertions.assertEquals(log.getTerm(),1);
                });


        logList.truncatePrefix(4);

        Assertions.assertEquals(logList.getLastLogIndex(),7);
        logList.getLog(5,6)
                .onSuccess(log->{
                    Assertions.assertEquals(1,log.size());
                    Assertions.assertEquals(log.get(0).getProposalId(),15);
                });

    }
}
