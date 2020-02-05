package com.stormpx;

import ch.qos.logback.classic.Level;
import ch.qos.logback.core.rolling.RollingFileAppender;
import ch.qos.logback.core.rolling.TimeBasedRollingPolicy;
import com.stormpx.kit.UnSafeJsonObject;
import io.netty.handler.logging.LogLevel;
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
                    return new ConfigStoreOptions().setType("file").setFormat("json").setConfig(new JsonObject().put("path", path));
                }).collect(Collectors.toList());

        ConfigRetrieverOptions options = new ConfigRetrieverOptions();
        options.setStores(configStoreOptions)
                .setScanPeriod(60*1000);

        vertx.eventBus().registerDefaultCodec(UnSafeJsonObject.class,UnSafeJsonObject.CODEC);
        int availableProcessors = Runtime.getRuntime().availableProcessors();
        logger.info("available processors :{}",availableProcessors);
        ConfigRetriever retriever = ConfigRetriever.create(vertx,options);
        Promise<Void> promise=Promise.promise();
        retriever.getConfig(ar->{
            try {
                if (ar.succeeded()){
                    JsonObject config = ar.result();
                    setLogLevel(config);
                    DeploymentOptions mqtt = new DeploymentOptions().setInstances(availableProcessors/2==0?1:availableProcessors/2).setConfig(config);

                    retriever.listen(configChange->{
                        logger.info("detected config change");
                        vertx.eventBus().publish(":config_change::",configChange, new DeliveryOptions().setLocalOnly(true));
                    });

                    vertx.deployVerticle(MqttBrokerVerticle.class,mqtt,arr->{
                        if (arr.succeeded()){
                            promise.complete();
                        }else{
                            promise.fail(arr.cause());
                        }
                    });
                }else{
                    promise.fail(ar.cause());
                }
            } catch (Exception e) {
                promise.fail(e);
            }
        });
        return promise.future();

    }

    private static void setLogLevel(JsonObject jsonObject){
        String info = jsonObject.getString("log-level", "info");
        ch.qos.logback.classic.Logger logger =
                (ch.qos.logback.classic.Logger)org.slf4j.LoggerFactory.getLogger(org.slf4j.Logger.ROOT_LOGGER_NAME);
        logger.setLevel(Level.toLevel(info));
        String log_dir = jsonObject.getString("log_dir");

        if (log_dir!=null){
            String dir = Paths.get(log_dir).normalize().toString();
            RollingFileAppender appender = (RollingFileAppender) logger.getAppender("");
            TimeBasedRollingPolicy rollingPolicy = (TimeBasedRollingPolicy) appender.getRollingPolicy();
            rollingPolicy.setFileNamePattern(dir+"/%d.log");
        }

    }

}
