package com.luck.entity;

import org.apache.hadoop.hbase.ServerName;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * @author luchengkai
 * @description regionServer实体类
 * @date 2022/5/20 14:17
 */
public class THServerInfo implements Serializable {
    ServerName ServerName;          // 分区服务器名
    int serverStatus = 0;           // server状态（-1：cold；0：normal；1：hot）
    boolean needMove = false;       // true -> server分区状态均正常，但server状态异常，需要往外/里move
    String tableName;               // 表名
    List<THRegionInfo> regionInfos; // 节点下分区信息
    long regionCount;               // 节点分区个数
    long sumHitCount;               // 节点分区的总查询命中次数
    long sumHitTime;                // 节点分区的查询性能
    long sumRegionSize;             // 节点分区的查询性能

    public ServerName getServerName() {
        return ServerName;
    }

    public void setServerName(ServerName serverName) {
        this.ServerName = serverName;
    }

    public int getServerStatus() {
        return serverStatus;
    }

    public void setServerStatus(int serverStatus) {
        this.serverStatus = serverStatus;
    }

    public boolean isNeedMove() {
        return needMove;
    }

    public void setNeedMove(boolean needMove) {
        this.needMove = needMove;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<THRegionInfo> getRegionInfos() {
        return regionInfos;
    }

    public void setRegionInfos(List<THRegionInfo> regionInfos) {
        this.regionInfos = regionInfos;
    }

    public long getRegionCount() {
        return regionCount;
    }

    public void setRegionCount(long regionCount) {
        this.regionCount = regionCount;
    }

    public long getSumHitCount() {
        return sumHitCount;
    }

    public void setSumHitCount(long sumHitCount) {
        this.sumHitCount = sumHitCount;
    }

    public long getSumHitTime() {
        return sumHitTime;
    }

    public void setSumHitTime(long sumHitTime) {
        this.sumHitTime = sumHitTime;
    }

    public long getSumRegionSize() {
        return sumRegionSize;
    }

    public void setSumRegionSize(long sumRegionSize) {
        this.sumRegionSize = sumRegionSize;
    }
}
