package com.luck.entity;

import com.luck.utils.ByteUtil;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.nio.charset.StandardCharsets;

/**
 * @author luchengkai
 * @description 降维索引结构体
 * @date 2021/12/7 21:09
 */
public class KeyInfo {
    private String timeKey;
    private String rangeKey;

    public KeyInfo(String timeKey, String rangeKey){
        this.rangeKey = rangeKey;
        this.timeKey = timeKey;
    }

    public String getTimeKey() {
        return timeKey;
    }

    public void setTimeKey(String timeKey) {
        this.timeKey = timeKey;
    }

    public String getRangeKey() {
        return rangeKey;
    }

    public void setRangeKey(String rangeKey) {
        this.rangeKey = rangeKey;
    }

    public byte[] toBytes(){
        ByteUtil byteUtil = new ByteUtil();
        byte[] timeKey = this.timeKey.getBytes(StandardCharsets.UTF_8);
        byte[] rangeKey = this.rangeKey.getBytes(StandardCharsets.UTF_8);
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
