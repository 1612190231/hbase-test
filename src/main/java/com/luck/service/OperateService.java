package com.luck.service;

import java.io.IOException;
import java.util.Map;

/**
 * @author luchengkai
 * @description Hbase接口类
 * @date 2021/4/20 13:52
 */
public interface OperateService {

    // 初始化
    void init();

    //创建表
    void createTable(String tableName, String seriesStr) throws IOException;

    //添加数据
    void add(String rowKey, Map<String, Object> columns) throws IOException;

    //根据rowkey获取数据
    Map<String, String> getAllValue(String rowKey) throws IOException;

    //根据rowkey和column获取数据
    String getValueBySeries(String rowKey, String column) throws IOException;

    //根据table查询所有数据
    void  getValueByTable() throws Exception;

    //删除表
    void dropTable(String tableName) throws IOException;
}
