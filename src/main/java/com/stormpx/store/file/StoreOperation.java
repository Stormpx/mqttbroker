package com.stormpx.store.file;

import java.util.Objects;

public class StoreOperation {
    private String id;
    // 1 save 2 delete
    private int operation;


    public StoreOperation(String id, int operation) {
        this.id = id;
        this.operation = operation;
    }

    public String getId() {
        return id;
    }

    public int getOperation() {
        return operation;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        StoreOperation that = (StoreOperation) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
