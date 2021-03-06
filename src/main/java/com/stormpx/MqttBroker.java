package com.stormpx;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import com.stormpx.cluster.ClusterVerticle;
import com.stormpx.cluster.mqtt.RetainMatchResult;
import com.stormpx.cluster.mqtt.SessionResult;
import com.stormpx.cluster.mqtt.TopicMatchResult;
import com.stormpx.cluster.mqtt.ActionLog;
import com.stormpx.cluster.mqtt.command.RequestMessageCommand;
import com.stormpx.cluster.mqtt.command.SendMessageCommand;
import com.stormpx.dispatcher.DispatcherVerticle;
import com.stormpx.dispatcher.EventContext;
import com.stormpx.dispatcher.MessageContext;
import com.stormpx.dispatcher.command.*;
import com.stormpx.kit.GeneralMessageCodec;
import com.stormpx.dispatcher.DispatcherMessage;
import com.stormpx.store.MessageLink;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.eventbus.DeliveryOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class MqttBroker {
    private final static Logger logger= LoggerFactory.getLogger(MqttBroker.class);


    public static Future<Void> start(Vertx vertx, String... args){
        CLI mqttbroker = CLI.create("mqttbroker");
        mqttbroker.addOption(new Option().setLongName("config").setShortName("c").setDescription("config file path").setMultiValued(true).setRequired(false));
        CommandLine line = mqttbroker.parse(Arrays.asList(args));
        List<ConfigStoreOptions> configStoreOptions = Optional.ofNullable(line.<String>getOptionValues("c")).orElseGet(Collections::emptyList)
                .stream()
                .map(path -> {
                    logger.info("config path:{}",path);
                    if (path.endsWith("json")){
                        return new ConfigStoreOptions().setType("file").setFormat("json").setConfig(new JsonObject().put("path", path));
                    }else{
                        return new ConfigStoreOptions().setType("file").setFormat("yaml").setConfig(new JsonObject().put("path", path));
                    }

                }).collect(Collectors.toList());

        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
        options.setStores(configStoreOptions)
                .setScanPeriod(60*1000);


        ConfigRetriever retriever = ConfigRetriever.create(vertx,options);
        Promise<Void> promise=Promise.promise();
        retriever.getConfig(ar->{
            try {
                if (ar.succeeded()){
                    retriever.listen(configChange->{
                        logger.info("detected config change");
                        vertx.eventBus().publish(":config_change::",configChange.toJson(), new DeliveryOptions().setLocalOnly(true));
                    });

                    start(vertx,ar.result()).setHandler(promise);
                }else{
                    promise.fail(ar.cause());
                }
            } catch (Exception e) {
                promise.fail(e);
            }
        });
        return promise.future();

    }


    public static Future<Void> start(Vertx vertx,JsonObject config){
        setLogLevel(config);

        registerCodec(vertx);

        if (clusterEnable(config)) {
            config.put("isCluster",true);
        }

        return deployDispatcherVerticle(vertx, config)
                .compose(v->tryDeployClusterVerticle(vertx,config))
                .compose(v->deployBrokerVerticle(vertx, config))
                .map((Void)null);

    }

    private static boolean clusterEnable(JsonObject config){
        JsonObject cluster = config.getJsonObject("cluster");
        if (cluster==null)
            cluster=new JsonObject();

        String id = cluster.getString("id");
        Integer port = cluster.getInteger("port");
        JsonObject nodes = cluster.getJsonObject("nodes");
        if (id==null||port==null||port<0||port>66535||nodes==null||nodes.isEmpty()){
            return false;
        }else{
            return true;
        }
    }

    private static void registerCodec(Vertx vertx){
        vertx.eventBus().registerDefaultCodec(DispatcherMessage.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(ClientAcceptCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(MessageContext.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(SubscriptionsCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(UnSubscriptionsCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(CloseSessionCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(MessageLink.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(PacketIdActionCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(TakenOverCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(EventContext.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(ActionLog.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(SessionResult.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(RetainMatchResult.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(RequestMessageCommand.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(TopicMatchResult.class, new GeneralMessageCodec<>(){});
        vertx.eventBus().registerDefaultCodec(SendMessageCommand.class, new GeneralMessageCodec<>(){});


    }


    private static Future<String> tryDeployClusterVerticle(Vertx vertx,JsonObject config){
        if (!config.getBoolean("isCluster",false)){
            return Future.succeededFuture("");
        }
        Promise<String> promise=Promise.promise();
        vertx.deployVerticle(ClusterVerticle.class,new DeploymentOptions().setConfig(config),promise);
        return promise.future();
    }

    private static Future<String> deployDispatcherVerticle(Vertx vertx,JsonObject config){
        Promise<String> promise=Promise.promise();
        vertx.deployVerticle(DispatcherVerticle.class,new DeploymentOptions().setConfig(config),promise);
        return promise.future();
    }

    private static Future<String> deployBrokerVerticle(Vertx vertx,JsonObject config){
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        logger.info("available processors :{}",availableProcessors);
        Integer verticleInstance = config.getInteger("verticle_instance", availableProcessors);
        config.put("verticle_instance",verticleInstance);
        logger.info("verticle instance :{}",verticleInstance);
        DeploymentOptions mqtt = new DeploymentOptions().setConfig(config);
        Promise<String> promise=Promise.promise();
        vertx.deployVerticle(MqttBrokerVerticle.class,mqtt,promise);
        return promise.future();
    }

    private static void setLogLevel(JsonObject jsonObject){
        String level = jsonObject.getString("log_level", "info");
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger)org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.info("log level:{}",Level.toLevel(level));
        logger.setLevel(Level.toLevel(level));
        String log_dir = jsonObject.getString("log_dir");

        if (log_dir!=null){
            String dir = Paths.get(log_dir).normalize().toString();
            logger.info("log dir:{}",dir);
            RollingFileAppender appender = (RollingFileAppender) logger.getAppender("FILE");
            TimeBasedRollingPolicy rollingPolicy = (TimeBasedRollingPolicy) appender.getRollingPolicy();
            rollingPolicy.setFileNamePattern(dir+"/%d{yyyy-MM-dd}.log");
            rollingPolicy.start();
        }else{
            logger.detachAppender("FILE");
        }
    }

}
