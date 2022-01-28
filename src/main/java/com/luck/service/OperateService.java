package com.luck.service;

import com.luck.entity.BaseInfo;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * @author luchengkai
 * @description Hbase接口类
 * @date 2021/4/20 13:52
 */
public interface OperateService {

    String getSeries();

    void setSeries(String series);

    String getTableName();

    void setTableName(String tableName);

    List<ServerName> getServerNames();

    void setServerNames() throws IOException;

    // 初始化
    void init();

    //创建表
    void createTable(String tableName, String seriesStr);

    // 创建表 - 预分区
    void createTable(String tableName, String seriesStr, byte[][] startKey);

    //添加数据---按列族单条
    void add(String columnFamily, String rowKey, Map<String, Object> columns);

    //添加数据---全表
    void addByRowKey(BaseInfo baseInfo);

    //根据rowkey获取数据
    Map<String, String> getAllValue(String rowKey);

    //根据rowkey和column获取数据
    String getValueBySeries(String rowKey, String column);

    //根据table查询所有数据
    ResultScanner getValueByTable();

    //根据rowKey前缀查询记录
    ResultScanner getValueByPreKey(String preRow);

    //删除表
    void dropTable(String tableName);

    // 计算分区价值
    long calculateRegionValue(Long rowKey) throws IOException;

    // 计算分区价值
    long calculateRegionValue(HRegionInfo hRegionInfo) throws IOException;

//    // 查询regions
    List<HRegionInfo> getRegions() throws IOException;
//
//    // 统计region的大小
    Map<String, RegionLoad> getRegionLoad(List<ServerName> serverNames) throws IOException;

}
