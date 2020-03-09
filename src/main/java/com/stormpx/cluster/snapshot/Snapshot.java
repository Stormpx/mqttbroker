package com.stormpx.cluster.snapshot;

import com.stormpx.kit.value.Values2;
import com.stormpx.kit.value.Values3;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.util.UUID;

public class Snapshot {
    private final static Logger logger= LoggerFactory.getLogger(Snapshot.class);
    private final static String SNAPSHOT_FILE="snapshot";
    private Vertx vertx;
    private int index;
    private int term;
    private String dir;

    private SnapshotWriter snapshotWriter;

    private boolean writeSnapshot;
    private boolean readSnapshot;

    public Snapshot(Vertx vertx, int index, int term, String dir) {
        this.vertx = vertx;
        this.index = index;
        this.term = term;
        this.dir = dir;
    }

    public Future<SnapshotWriter> newWriter(String id, int index, int term){
        writeSnapshot =true;
        return createTempFile()
                .compose(path->openFile(path).map(af-> Values2.values(path,af)))
                .map(value->{
                    String path = value.getOne();
                    AsyncFile af = value.getTwo();
                    SnapshotWriter snapshotWriter = new SnapshotWriter(id,af);
                    snapshotWriter.future()
                            .setHandler(v->{
                                if (v.succeeded()&&id.equals(snapshotWriter.getId())){
                                    vertx.fileSystem().move(path,dir + File.separator + SNAPSHOT_FILE,new CopyOptions().setAtomicMove(true).setReplaceExisting(true),arr->{
                                        if (arr.succeeded()){
                                            this.index=index;
                                            this.term=term;
                                        }else{
                                            logger.error("move snapshot failed",arr.cause());
                                            vertx.fileSystem().delete(path,r->{});
                                        }
                                        drop(id);
                                    });
                                }else{
                                    drop(id);
                                    vertx.fileSystem().delete(path,r->{});
                                }
                            });
                    this.snapshotWriter=snapshotWriter;
                    return snapshotWriter;
                })
                .onFailure(t->writeSnapshot=false);
    }

    public void drop(String id){
        if (this.snapshotWriter.getId().equals(id)){
            this.snapshotWriter=null;
            this.writeSnapshot=false;
        }
    }

    private Future<String> createTempFile(){
        Promise<String> promise=Promise.promise();
        vertx.fileSystem().createTempFile(dir,"snapshot",".snapshot",null,promise);
        return promise.future();
    }

    private Future<AsyncFile> openFile(String path){
        Promise<AsyncFile> promise=Promise.promise();
        vertx.fileSystem().open(path,new OpenOptions().setWrite(true),promise);
        return promise.future();
    }

    public SnapshotWriter writer(){
        return snapshotWriter;
    }

    public boolean snapshotting(){
        return snapshotWriter!=null;
    }

}
