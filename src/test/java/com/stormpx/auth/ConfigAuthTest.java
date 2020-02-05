package com.stormpx.auth;

import com.stormpx.MqttBrokerVerticle;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.ExtendWith;

import static com.stormpx.Constants.TCP;

@ExtendWith(VertxExtension.class)
public class ConfigAuthTest {



    @BeforeAll
    static void beforeClass(Vertx vertx, VertxTestContext context) {
        System.setProperty(LoggerFactory.LOGGER_DELEGATE_FACTORY_CLASS_NAME,"io.vertx.core.logging.SLF4JLogDelegateFactory");
        LoggerFactory.initialise();
        JsonArray users = new JsonArray();
        users.add(
                new JsonObject()
                    .put("username","client1")
                    .put("password","password")
        ).add(
            new JsonObject()
                .put("username","client2")
                .put("password","password1")
                .put("ip","127.0.0.1")
        );
        DeploymentOptions mqtt = new DeploymentOptions().setConfig(new JsonObject().put("auth","config").put(TCP,new JsonObject().put("port",11883)));
        vertx.deployVerticle(new MqttBrokerVerticle(), mqtt,context.succeeding(v->{

           /* client2 = MqttClient.builder()
                    .identifier(UUID.randomUUID().toString())
                    .serverHost("localhost")
                    .serverPort(11883)
                    .useMqttVersion5()
                    .willPublish(Mqtt5Publish.builder().topic("/will/test").qos(MqttQos.EXACTLY_ONCE).payload("qwewq".getBytes(StandardCharsets.UTF_8)).asWill().build())
                    .build().toBlocking();
            client2.connect();*/

            context.completeNow();

        }));

    }

}
