package com.luck.index.splitkey.impl;

import com.luck.index.splitkey.SplitKeysCalculator;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.Iterator;
import java.util.TreeSet;

public class ShardSplitKeysCalculator implements SplitKeysCalculator {
    @Override
    public byte[][] calcSplitKeys() {
        return new byte[0][];
    }

    @Override
    public byte[][] getSplitKeys(int regionNum) {
        //1.构建前缀
        String[] keys = new String[regionNum];
        for(int i=0;i<regionNum;i++){
            String pre = StringUtils.leftPad(String.valueOf(i), 4, "0") + "|";

            keys[i] = pre;
        }
        //2.构建一个返回值数组
        byte[][] splitKeys = new byte[keys.length][];

        //使用一个有序集合封装前缀
        TreeSet<byte[]> row = new TreeSet<byte[]>();
        for (String key : keys) {
            row.add(Bytes.toBytes(key));
        }
        //迭代TreeSet,把值赋值给splitKeys
        Iterator<byte[]> iterator = row.iterator();
        int i =0;
        while(iterator.hasNext()){
            byte[] tempRow = iterator.next();
            iterator.remove();
            splitKeys[i] = tempRow;
            i++;
        }
        row.clear();
        row=null;
        return splitKeys;
    }
}
