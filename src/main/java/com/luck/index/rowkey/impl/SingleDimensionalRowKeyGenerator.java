package com.luck.index.rowkey.impl;

import com.luck.index.rowkey.RowKeyGenerator;
import org.apache.hadoop.hbase.util.Bytes;

public class SingleDimensionalRowKeyGenerator implements RowKeyGenerator {
    @Override
    public byte[] nextId() {
        return new byte[0];
    }

    @Override
    public byte[] nextId(Object o) {
        return Bytes.toBytes((long)o);
    }
}
