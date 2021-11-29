package com.luck.index.rowkey;

public interface RowKeyGenerator {
    byte [] nextId();

    byte [] nextId(Object o);
}
