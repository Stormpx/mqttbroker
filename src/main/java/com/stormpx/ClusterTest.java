package com.stormpx;

import com.stormpx.cluster.MqttCluster;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;

import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ClusterTest {
    public static void main(String[] args) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        Vertx vertx=Vertx.vertx();
        JsonObject json = new JsonObject();
        if (args[0].equals("56421"))
            json.put("id","machine1");

        if (args[0].equals("56422"))
            json.put("id","machine2");

        if (args[0].equals("56423"))
            json.put("id","machine3");

        json.put("port",Integer.valueOf(args[0]));
        JsonObject entries = new JsonObject();
        json.put("nodes",entries);
        if (!args[0].equals("56421"))
            entries.put("machine1","127.0.0.1:56421");

        if (!args[0].equals("56422"))
            entries.put("machine2","127.0.0.1:56422");

        if (!args[0].equals("56423"))
            entries.put("machine3","127.0.0.1:56423");

        new MqttCluster(vertx,json)
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
