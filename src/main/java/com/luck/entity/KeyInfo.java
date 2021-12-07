package com.luck.entity;

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
}
