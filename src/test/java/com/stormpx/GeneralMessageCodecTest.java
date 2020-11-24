package com.stormpx;

import com.stormpx.kit.GeneralMessageCodec;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
public class GeneralMessageCodecTest {

    @Test
    public void test(Vertx vertx, VertxTestContext vertxTestContext){
        vertx.eventBus().registerDefaultCodec(GeneralMessageCodecTest.class,new GeneralMessageCodec(){});

        vertx.eventBus().<GeneralMessageCodecTest>localConsumer("test")
                .handler(msg->{
                    System.out.println(msg.body());
                    vertxTestContext.completeNow();
                });

        vertx.eventBus().send("test",new GeneralMessageCodecTest());

    }

}
