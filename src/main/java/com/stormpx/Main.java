package com.stormpx;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import io.vertx.config.ConfigRetriever;
import io.vertx.config.ConfigRetrieverOptions;
import io.vertx.config.ConfigStoreOptions;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.cli.Argument;
import io.vertx.core.cli.CLI;
import io.vertx.core.cli.CommandLine;
import io.vertx.core.cli.Option;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class Main {
    public static void main(String[] args) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");

        LoggerContext loggerContext = (LoggerContext) org.slf4j.LoggerFactory.getILoggerFactory();
        loggerContext.getLogger("test").setLevel(Level.DEBUG);

        Vertx vertx = Vertx.vertx();
        vertx.exceptionHandler(Throwable::printStackTrace);
        MqttBroker.start(vertx,args)
                .setHandler(ar->{
                    if (ar.failed()) {
                        ar.cause().printStackTrace();
                        vertx.close();
                    }
                });

    }

}
