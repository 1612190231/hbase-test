package com.luck.index;

import com.luck.index.rowkey.RowKeyGenerator;
import com.luck.index.splitkey.SplitKeysCalculator;
import org.apache.hadoop.hbase.util.Bytes;

public class PartitionRowKeyManager implements RowKeyGenerator,
        SplitKeysCalculator {

    public static final int DEFAULT_PARTITION_AMOUNT = 20;
    private long currentId = 1;
    private int partition = DEFAULT_PARTITION_AMOUNT;
    public void setPartition(int partition) {
        this.partition = partition;
    }

    public byte[] nextId() {
        try {
            long partitionId = currentId % partition;
            return Bytes.add(Bytes.toBytes(partitionId),
                    Bytes.toBytes(currentId));
        } finally {
            currentId++;
        }
    }

    @Override
    public byte[] nextId(Object o) {
        return new byte[0];
    }

    public byte[][] calcSplitKeys() {
        byte[][] splitKeys = new byte[partition - 1][];
        for(int i = 1; i < partition ; i ++) {
            splitKeys[i-1] = Bytes.toBytes((long)i);
        }
        return splitKeys;
    }
}
