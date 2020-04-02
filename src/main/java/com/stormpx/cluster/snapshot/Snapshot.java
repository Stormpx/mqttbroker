package com.stormpx.cluster.snapshot;

import com.stormpx.kit.value.Values2;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.file.AsyncFile;
import io.vertx.core.file.CopyOptions;
import io.vertx.core.file.OpenOptions;
import io.vertx.core.json.Json;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class Snapshot {
    private final static Logger logger= LoggerFactory.getLogger(Snapshot.class);
    private final static String SNAPSHOT_FILE="snapshot";

    private Vertx vertx;

    private SnapshotMeta snapshotMeta;

    private String dir;

    private AsyncFile asyncFile;

    private SnapshotContext snapshotContext;

    private boolean writeSnapshot;

    private Map<String,SnapshotReader> snapshotReaderMap;

    private Handler<SnapshotMeta> snapshotHandler;

    private Handler<Void> safetyHandler;

    private Promise<AsyncFile> promise=Promise.promise();

    public Snapshot(Vertx vertx,String id, int index, int term, String dir) {
        this.vertx = vertx;
        this.snapshotMeta=new SnapshotMeta(id,index,term);
        this.snapshotReaderMap=new HashMap<>();
        this.dir = dir;
        if (index!=0) {
            reOpenSnapshot();
        }else{
            this.promise.tryComplete();
        }
    }

    private void reOpenSnapshot(){
        openFile(dir + File.separator + SNAPSHOT_FILE)
                .onFailure(t ->{
                    logger.error("open snapshot failed retry", t);
                    reOpenSnapshot();
                })
                .onSuccess(af->{
                    this.asyncFile=af;
                    promise.tryComplete(af);
                });
    }

    public void readDone(String nodeId){
        snapshotReaderMap.remove(nodeId);
        if (snapshotReaderMap.isEmpty()){
            promise.future()
                    .onSuccess(af->{
                        if (!snapshotReaderMap.isEmpty())
                            return;
                        Handler<Void> safetyHandler = this.safetyHandler;

                        if (safetyHandler==null)
                            return;


                        if (this.promise.future().isComplete()) {
                            this.promise = Promise.promise();
                            this.asyncFile = null;
                        }
                        if (meta().getIndex()==0){
                            logger.debug("current snapshot lastIncludeIndex is zero ");
                            safetyHandler.handle(null);
                        }else {

                            af.close(v -> {
                                if (v.failed()) {
                                    logger.error("file close failed?", v.cause());
                                }
                                safetyHandler.handle(null);
                            });
                        }
                    });
        }
    }

    private void onSafe(Handler<Void> handler) {
        this.safetyHandler = handler;
        readDone(null);
    }

    public Future<SnapshotReader> reader(String nodeId){
        SnapshotReader reader = snapshotReaderMap.get(nodeId);
        if (reader!=null){
            return Future.succeededFuture(reader);
        }
        return promise.future()
                .compose(af->{
                    //may closed
                    if (asyncFile==null)
                        //closed wait
                        return reader(nodeId);

                    //check again
                    SnapshotReader r = snapshotReaderMap.get(nodeId);
                    if (r!=null){
                        return Future.succeededFuture(r);
                    }
                    SnapshotReader snapshotReader = new SnapshotReader(this,nodeId, af);
                    snapshotReaderMap.put(nodeId,snapshotReader);
                    return Future.succeededFuture(snapshotReader);
                });

    }


    private void handleWriterFuture(SnapshotWriter snapshotWriter){
        snapshotWriter.future().setHandler(ar -> {
            String path = snapshotWriter.getPath();
            SnapshotMeta snapshotMeta = snapshotWriter.getSnapshotMeta();
            Promise<Void> promise = ar.result();

            if (ar.succeeded() && this.snapshotContext!=null&&this.snapshotContext.getSnapshotMeta().getNodeId().equals(snapshotMeta.getNodeId())) {
                // wait all reader done
                onSafe(v->{
                    drop(snapshotMeta.getNodeId());
                    if (snapshotMeta.getIndex()<this.snapshotMeta.getIndex()){
                        promise.tryFail(String.format("snapshot index:%s older than current snapshot index:%s",snapshotMeta.getIndex(),this.snapshotMeta.getIndex()));
                        reOpenSnapshot();
                        return;
                    }
                    vertx.fileSystem().move(path, dir + File.separator + SNAPSHOT_FILE, new CopyOptions().setAtomicMove(true).setReplaceExisting(true), arr -> {
                        reOpenSnapshot();
                        if (arr.succeeded()) {
                            this.snapshotMeta = snapshotMeta;
                            Handler<SnapshotMeta> snapshotHandler = this.snapshotHandler;
                            if (snapshotHandler != null) {
                                snapshotHandler.handle(this.snapshotMeta);
                            }

                            promise.tryComplete();

                        } else {
                            logger.error("move {} to {} failed", arr.cause(), path, dir + File.separator + SNAPSHOT_FILE);
                            vertx.fileSystem().delete(path, r -> { });

                            promise.tryFail(arr.cause());
                        }
                    });
                    this.safetyHandler=null;
                });
            } else {
                if (ar.succeeded()) {
                    if (this.snapshotContext==null) {
                        promise.tryFail("write finished");
                    }else{
                        promise.tryFail(String.format("current snapshotWriter nodeId:%s != writer nodeId %s",
                                this.snapshotContext.getSnapshotMeta().getNodeId(), snapshotMeta.getNodeId()));
                    }
                }
                drop(snapshotMeta.getNodeId());
                vertx.fileSystem().delete(path, r -> { });
            }
        });

    }

    public SnapshotContext createWriterContext(String id, int index, int term){
        writeSnapshot = true;
        SnapshotMeta snapshotMeta = new SnapshotMeta(id, index, term);
        SnapshotContext snapshotContext = new SnapshotContext(snapshotMeta);

        SnapshotContext context = this.snapshotContext;
        if (context!=null)
            context.getWriter().onSuccess(SnapshotWriter::end);
        this.snapshotContext=snapshotContext;
        Future<SnapshotWriter> snapshotWriterFuture = createTempFile()
                .compose(path -> openFile(path).map(af -> Values2.values(path, af)))
                .map(value -> {
                    String path = value.getOne();
                    AsyncFile af = value.getTwo();
                    SnapshotWriter snapshotWriter = new SnapshotWriter(snapshotMeta, path,af);
                    handleWriterFuture(snapshotWriter);

                    return snapshotWriter;
                }).onFailure(t -> writeSnapshot = false);

        snapshotContext.setWriterFuture(snapshotWriterFuture);



        return snapshotContext;

    }

    public void drop(String id){
        if (this.snapshotContext!=null&&this.snapshotContext.getSnapshotMeta().getNodeId().equals(id)){
            this.snapshotContext=null;
            this.writeSnapshot=false;
        }
    }

    public void close(){
        if (this.promise.future().isComplete())
            this.promise=Promise.promise();
        if (asyncFile!=null)
            asyncFile.close();
    }

    public SnapshotContext writerContext(){
        return snapshotContext;
    }

    public boolean snapshotting(){
        return writeSnapshot;
    }


    public SnapshotMeta meta() {
        return snapshotMeta;
    }

    public Snapshot snapshotHandler(Handler<SnapshotMeta> snapshotHandler) {
        this.snapshotHandler = snapshotHandler;
        return this;
    }




    private Future<String> createTempFile(){
        Promise<String> promise=Promise.promise();
        vertx.fileSystem().createTempFile(dir,"snapshot",".snapshot",null,promise);
        return promise.future();
    }

    private Future<AsyncFile> openFile(String path){
        Promise<AsyncFile> promise=Promise.promise();
        vertx.fileSystem().open(path,new OpenOptions().setCreate(true).setWrite(true),promise);
        return promise.future();
    }
}
