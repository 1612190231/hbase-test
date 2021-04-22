package com.luck.service;

import com.luck.entity.BaseInfo;

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

    // 初始化
    void init();

    //创建表
    void createTable(String tableName, String seriesStr);

    //添加数据---按列族单条
    void add(String columnFamily, String rowKey, Map<String, Object> columns);

    //添加数据---全表
    void addByRowKey(BaseInfo baseInfo);

    //根据rowkey获取数据
    Map<String, String> getAllValue(String rowKey);

    //根据rowkey和column获取数据
    String getValueBySeries(String rowKey, String column);

    //根据table查询所有数据
    void  getValueByTable();

    //删除表
    void dropTable(String tableName);
}
