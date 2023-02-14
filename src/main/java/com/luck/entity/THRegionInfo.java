package com.luck.entity;

import java.io.Serializable;

/**
 * @author luchengkai
 * @description region实体类
 * @date 2022/5/20 14:17
 */
public class THRegionInfo implements Serializable {
    String regionName;          // 分区名
    int regionStatus = 0;       // 分区状态(-1 -> 偏小，须merge； 1 -> 偏大，须split）
    boolean hasDeal = false;    // region是否被操作过
    String tableNmae;           // 表名
    long regionSize;            // 分区大小
    long decayTime;             // 衰减时间量
    long hitCount;              // 分区查询命中次数
    String startKey;            // 开始键
    String endKey;              // 结束键

    public String getRegionName() {
        return regionName;
    }

    public void setRegionName(String regionName) {
        this.regionName = regionName;
    }

    public int getRegionStatus() {
        return regionStatus;
    }

    public void setRegionStatus(int regionStatus) {
        this.regionStatus = regionStatus;
    }

    public String getTableNmae() {
        return tableNmae;
    }

    public void setTableNmae(String tableNmae) {
        this.tableNmae = tableNmae;
    }

    public long getRegionSize() {
        return regionSize;
    }

    public void setRegionSize(long regionSize) {
        this.regionSize = regionSize;
    }

    public long getDecayTime() {
        return decayTime;
    }

    public void setDecayTime(long decayTime) {
        this.decayTime = decayTime;
    }

    public long getHitCount() {
        return hitCount;
    }

    public void setHitCount(long hitCount) {
        this.hitCount = hitCount;
    }

    public String getStartKey() {
        return startKey;
    }

    public void setStartKey(String startKey) {
        this.startKey = startKey;
    }

    public String getEndKey() {
        return endKey;
    }

    public void setEndKey(String endKey) {
        this.endKey = endKey;
    }

    public boolean isHasDeal() {
        return hasDeal;
    }

    public void setHasDeal(boolean hasDeal) {
        this.hasDeal = hasDeal;
    }
}
