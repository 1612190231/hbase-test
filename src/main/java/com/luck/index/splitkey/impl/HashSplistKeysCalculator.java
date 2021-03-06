package com.luck.index.splitkey.impl;

import com.luck.index.rowkey.RowKeyGenerator;
import com.luck.index.rowkey.impl.HashRowKeyGenerator;
import com.luck.index.splitkey.SplitKeysCalculator;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;

public class HashSplistKeysCalculator implements SplitKeysCalculator {
    //随机取机数目
    private int baseRecord;
    //rowkey生成器
    private RowKeyGenerator rkGen;
    //取样时，由取样数目及region数相除所得的数量.
    private int splitKeysBase;
    //splitkeys个数
    private int splitKeysNumber;
    //由抽样计算出来的splitkeys结果
    private byte[][] splitKeys;

    public HashSplistKeysCalculator(int baseRecord, int prepareRegions) {
        this.baseRecord = baseRecord;
        //实例化rowkey生成器
        this.rkGen = new HashRowKeyGenerator();
        this.splitKeysNumber = prepareRegions - 1;
        this.splitKeysBase = baseRecord / prepareRegions;
    }

    public byte[][] calcSplitKeys() {
        splitKeys = new byte[splitKeysNumber][];
        //使用treeset保存抽样数据，已排序过
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);
        for (int i = 0; i < baseRecord; i++) {
            rows.add(rkGen.nextId());
        }
        int pointer = 0;
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int index = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            if ((pointer != 0) && (pointer % splitKeysBase == 0)) {
                if (index < splitKeysNumber) {
                    splitKeys[index] = tempRow;
                    index ++;
                }
            }
            pointer ++;
        }
        rows.clear();
        rows = null;
        return splitKeys;
    }

    @Override
    public byte[][] getSplitKeys(int regionNum) {
        return new byte[0][];
    }
}
