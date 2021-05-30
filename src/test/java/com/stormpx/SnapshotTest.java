package com.stormpx;

import com.stormpx.cluster.snapshot.Snapshot;
import com.stormpx.cluster.snapshot.SnapshotContext;
import com.stormpx.cluster.snapshot.SnapshotReader;
import com.stormpx.kit.FileUtil;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.junit5.Checkpoint;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

@ExtendWith(VertxExtension.class)
public class SnapshotTest {


    Buffer chunk1 = Buffer.buffer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaweqweeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee");
    Buffer chunk2 = Buffer.buffer("qwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwfdffffffffffffffffffffffffffffffffff");
    Buffer chunk3 = Buffer.buffer("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww");
    Buffer chunk4 = Buffer.buffer("fwaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    Buffer chunk5 = Buffer.buffer("22222222222222222222222222222222222222222222222222222222ewqqqqqqqqqqqqq");

    Buffer buffer=Buffer.buffer()
            .appendBuffer(chunk1)
            .appendBuffer(chunk2)
            .appendBuffer(chunk3)
            .appendBuffer(chunk4)
            .appendBuffer(chunk5);

    @Test
    public void test(Vertx vertx, VertxTestContext context) throws IOException, InterruptedException {
        String dir = System.getProperty("user.dir");
        FileUtil.create(new File(dir+"/snapshot"));
        Snapshot snapshot = new Snapshot(vertx, "node1", 0, 0, dir + "/snapshot");

        snapshot.snapshotHandler(meta->{
            Assertions.assertEquals(meta.getIndex(),100);
            Assertions.assertEquals(meta.getTerm(),2);
            System.out.println(Json.encodePrettily(meta));
            snapshot.reader("node1")
                    .setHandler(context.succeeding(reader->{
                        reader.readAll()
                                .setHandler(context.succeeding(b->{
                                    System.out.println(b.toString());
                                    System.out.println(buffer.toString());
                                    Assertions.assertEquals(b.toString(),buffer.toString());
                                    reader.done();

                                    readerTest(snapshot)
                                            .setHandler(context.succeeding(v->{
                                                snapshot.close();
                                                nextTest(vertx,context);
                                            }));

                                }));

                    }));
        });
        SnapshotContext writerContext = snapshot.createWriterContext("node2", 100, 2);

        writerContext.getWriter()
                .setHandler(context.succeeding(writer->{
                    System.out.println("start write");
                    writer.write(chunk1);
                    writer.write(chunk2);
                    writer.write(chunk3);
                    writer.write(chunk4);
                    writer.write(chunk5);
                    writer.end()
                            .setHandler(ar->{
                                if (ar.failed()){
                                    context.failNow(ar.cause());
                                }
                            });
                }));

        boolean b = context.awaitCompletion(30, TimeUnit.SECONDS);

        System.out.println("done");

    }

    public Future<Void> readerTest(Snapshot snapshot){
        Promise<Void> promise=Promise.promise();
        snapshot.reader("node2")
                .onFailure(promise::tryFail)
                .onSuccess(reader->{
                    reader.readAll()
                            .onFailure(promise::tryFail)
                            .onSuccess(b->{
                                Assertions.assertEquals(b.toString(),buffer.toString());
                                reader.done();
                                promise.complete();
                            });
                });
        return promise.future();

    }

    public void nextTest(Vertx vertx, VertxTestContext context){
        String dir = System.getProperty("user.dir");
        Snapshot snapshot = new Snapshot(vertx, "node2", 100, 2, dir+"/snapshot");
        Checkpoint checkpoint = context.checkpoint(2);

        byte[] bytes = new byte[4096];

        Arrays.fill(bytes, (byte) 0x40);

        Buffer buffer1 = Buffer.buffer(bytes);
        Buffer buffer2 = Buffer.buffer(bytes);

        snapshot.snapshotHandler(meta->{
            Assertions.assertEquals(meta.getIndex(),1000);
            Assertions.assertEquals(meta.getTerm(),5);
            System.out.println(Json.encodePrettily(meta));
            snapshot.reader("node1")
                    .setHandler(context.succeeding(reader->{
                        reader.readAll()
                                .setHandler(context.succeeding(b->{
                                    Assertions.assertArrayEquals(b.getBytes(),Buffer.buffer().appendBuffer(buffer1).appendBuffer(buffer2).getBytes());
                                    reader.done();
                                    snapshot.close();
                                    checkpoint.flag();

                                }));

                    }));

        });

        readerTest(snapshot)
                .setHandler(context.succeeding(v->{
                    System.out.println("readtest2 suc");
                    checkpoint.flag();
                }));



        SnapshotContext writerContext = snapshot.createWriterContext("node2", 1000, 5);

        writerContext.getWriter()
                .setHandler(context.succeeding(writer->{
                    System.out.println("start write2");
                    writer.write(buffer1);
                    writer.write(buffer2);
                    writer.end()
                            .setHandler(ar->{
                                if (ar.failed()){
                                    context.failNow(ar.cause());
                                }
                            });
                }));

    }


    @Test
    public void competitionTest(Vertx vertx,VertxTestContext context) throws InterruptedException {
        String dir = System.getProperty("user.dir");
        FileUtil.create(new File(dir+"/snapshot"));
        Snapshot snapshot = new Snapshot(vertx, "node1", 0, 2, dir + "/snapshot");
        Checkpoint checkpoint = context.checkpoint(3);
        Buffer correctBuffer=Buffer.buffer("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

        snapshot.snapshotHandler(meta->{
            Assertions.assertEquals(meta.getIndex(),200);
            Assertions.assertEquals(meta.getTerm(),3);
            Assertions.assertEquals(meta.getNodeId(),"node3");
            System.out.println("final "+Json.encodePrettily(meta));
            snapshot.reader("node1")
                    .setHandler(context.succeeding(reader->{
                        reader.readAll()
                                .setHandler(context.succeeding(b->{
                                    System.out.println(b.toString());
                                    Assertions.assertEquals(b.toString(),correctBuffer.toString());
                                    Assertions.assertNull(snapshot.writerContext());
                                    Assertions.assertFalse(snapshot.snapshotting());
                                    reader.done();
                                    checkpoint.flag();
                                }));

                    }));
        });

        snapshot.createWriterContext("node2",150,2)
                .getWriter()
                .setHandler(context.succeeding(writer->{
                    System.out.println("123");
                    if (writer.isEnd()) {
                        checkpoint.flag();
                    } else {
                        writer.write(Buffer.buffer("whatever"));
                        writer.end().setHandler(context.failing(v -> {
                            checkpoint.flag();
                        }));
                    }
                }));

        snapshot.createWriterContext("node3",200,3)
                .getWriter()
                .setHandler(context.succeeding(writer->{
                    System.out.println(222);
                    writer.write(correctBuffer);
                    if (writer.isEnd()){
                        checkpoint.flag();
                    }else {
                        writer.end().setHandler(context.succeeding(v -> {
                            checkpoint.flag();
                        }));
                    }
                }));


    }



}
