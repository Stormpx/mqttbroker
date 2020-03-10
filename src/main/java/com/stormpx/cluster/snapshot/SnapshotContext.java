package com.stormpx.cluster.snapshot;

import io.vertx.core.Future;

public class SnapshotContext {
    private SnapshotMeta snapshotMeta;
    private Future<SnapshotWriter> snapshotWriterFuture;


    public SnapshotContext(SnapshotMeta snapshotMeta) {
        this.snapshotMeta = snapshotMeta;
    }

    public Future<SnapshotWriter> getWriter() {
        return snapshotWriterFuture;
    }

    public SnapshotContext setWriterFuture(Future<SnapshotWriter> snapshotWriterFuture) {
        this.snapshotWriterFuture = snapshotWriterFuture;
        return this;
    }

    public SnapshotMeta getSnapshotMeta() {
        return snapshotMeta;
    }
}
