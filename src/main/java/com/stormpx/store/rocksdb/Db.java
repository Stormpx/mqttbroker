package com.stormpx.store.rocksdb;

import com.stormpx.kit.FileUtil;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Db {
    private static RocksDB rocksDB;
    private static ColumnFamilyHandle MESSAGE_COLUMN_FAMILY;
    private static ColumnFamilyHandle SESSION_COLUMN_FAMILY;
    private static ColumnFamilyHandle CLUSTER_COLUMN_FAMILY;
    private static WriteOptions ASYNC_WRITE_OPTIONS;

    public static void initialize(String dir) throws RocksDBException {
        String path = Paths.get(dir).normalize().toString();
        FileUtil.create(new File(path));

        DBOptions options = new DBOptions();
        options.setCreateIfMissing(true);
        options.setCreateMissingColumnFamilies(true);
        RocksEnv.getDefault().setBackgroundThreads(4);
        options.setMaxBackgroundFlushes(2);
        options.setBaseBackgroundCompactions(2);
        options.setEnableWriteThreadAdaptiveYield(true);
        List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY),
                new ColumnFamilyDescriptor("message".getBytes(),new ColumnFamilyOptions().setLevelCompactionDynamicLevelBytes(true).setMaxWriteBufferNumber(6).setWriteBufferSize(256*SizeUnit.MB)),
                new ColumnFamilyDescriptor("session".getBytes(),new ColumnFamilyOptions().setLevelCompactionDynamicLevelBytes(true).setMaxWriteBufferNumber(4).setWriteBufferSize(128*SizeUnit.MB)),
                new ColumnFamilyDescriptor("cluster".getBytes(),new ColumnFamilyOptions().setLevelCompactionDynamicLevelBytes(true).setMaxWriteBufferNumber(4).setWriteBufferSize(128*SizeUnit.MB))
        );
        List<ColumnFamilyHandle> columnFamilyHandles=new ArrayList<>();
        rocksDB=RocksDB.open(options,path,cfDescriptors,columnFamilyHandles);
        for (ColumnFamilyHandle columnFamilyHandle : columnFamilyHandles) {
            if (new String(columnFamilyHandle.getName()).equals("message")){

                MESSAGE_COLUMN_FAMILY=columnFamilyHandle;

            }else if (new String(columnFamilyHandle.getName()).equals("session")){

                SESSION_COLUMN_FAMILY=columnFamilyHandle;

            }else if (new String(columnFamilyHandle.getName()).equals("cluster")){

                CLUSTER_COLUMN_FAMILY=columnFamilyHandle;
            }

        }
        ASYNC_WRITE_OPTIONS=new WriteOptions().setSync(false);
    }

    public static void destroy(){
        if (rocksDB!=null) {
            rocksDB.getDefaultColumnFamily().close();
            messageColumnFamily().close();
            sessionColumnFamily().close();
            clusterColumnFamily().close();
            rocksDB.close();
        }
    }

    public static RocksDB db(){
        return rocksDB;
    }


    public static ColumnFamilyHandle messageColumnFamily() {
        return MESSAGE_COLUMN_FAMILY;
    }

    public static ColumnFamilyHandle sessionColumnFamily() {
        return SESSION_COLUMN_FAMILY;
    }

    public static ColumnFamilyHandle clusterColumnFamily() {
        return CLUSTER_COLUMN_FAMILY;
    }

    public static WriteOptions asyncWriteOptions() {
        return ASYNC_WRITE_OPTIONS;
    }
}
