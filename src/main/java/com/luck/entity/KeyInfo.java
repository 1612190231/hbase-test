package com.luck.entity;

import com.luck.utils.ByteUtil;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

/**
 * @author luchengkai
 * @description 降维索引结构体
 * @date 2021/12/7 21:09
 */
public class KeyInfo {
    private Long timeKey;
    private Long rangeKey;

    public KeyInfo(Long timeKey, Long rangeKey){
        this.rangeKey = rangeKey;
        this.timeKey = timeKey;
    }

    public Long getTimeKey() {
        return timeKey;
    }

    public void setTimeKey(Long timeKey) {
        this.timeKey = timeKey;
    }

    public Long getRangeKey() {
        return rangeKey;
    }

    public void setRangeKey(Long rangeKey) {
        this.rangeKey = rangeKey;
    }

    public byte[] toBytes(){
        ByteUtil byteUtil = new ByteUtil();
        byte[] timeKey = byteUtil.convertLongToBytes(this.timeKey);
        byte[] rangeKey = byteUtil.convertLongToBytes(this.rangeKey);
        return byteUtil.mergeBytes(timeKey, rangeKey);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        KeyInfo keyInfo = (KeyInfo) o;
        if (!timeKey.equals(keyInfo.timeKey)) return false;
        return rangeKey.equals(keyInfo.rangeKey);
    }

    @Override
    public int hashCode() {
        int result = timeKey.hashCode();
        result = 31 * result + rangeKey.hashCode();
        return result;
    }
}
