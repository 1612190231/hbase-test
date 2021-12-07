package com.luck.service.impl;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;


import com.luck.entity.BaseInfo;
import com.luck.service.OperateService;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class OperateServiceImpl implements OperateService {
    private Logger logger = Logger.getLogger(this.getClass());

    private String series;  //列族
    private String tableName;   //表名
    private static Connection conn;

    public String getSeries() { return series; }

    public void setSeries(String series) { this.series = series; }

    public String getTableName() { return tableName; }

    public void setTableName(String tableName) { this.tableName = tableName; }

    public void init() {
        Configuration config = HBaseConfiguration.create();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.addResource("/src/main/resources/hbase-site.xml");
        try {
            logger.info("==========init start==========");
            conn = ConnectionFactory.createConnection(config);
//            createTable(tableName, series);
            logger.info("===========init end===========");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e);
            logger.info("==========init error==========");
        }
    }

    //创建表
    public void createTable(String tableName, String seriesStr) throws IllegalArgumentException {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            logger.info("==========create start==========");
            admin = conn.getAdmin();
            if (!admin.tableExists(table)) {
                System.out.println(tableName + " table not Exists");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] series = seriesStr.split(",");
                for (String s : series) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                admin.createTable(descriptor);
                logger.info("==========create success==========");
            }
            logger.info("===========create end===========");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
            logger.info("==========create error==========");
        }
        finally {
            IOUtils.closeQuietly(admin);
        }
    }

    //创建表---预分区
    public void createTable(String tableName, String seriesStr, byte[][] startKey) throws IllegalArgumentException {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            logger.info("==========create start==========");
            admin = conn.getAdmin();
            if (!admin.tableExists(table)) {
                System.out.println(tableName + " table not Exists");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] series = seriesStr.split(",");
                for (String s : series) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                admin.createTable(descriptor, startKey);
                logger.info("==========create success==========");
            }
            logger.info("===========create end===========");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
            logger.info("==========create error==========");
        }
        finally {
            IOUtils.closeQuietly(admin);
        }
    }

    //添加数据---按列族
    public void add(String columnFamily, String rowKey, Map<String, Object> columns) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, Object> entry : columns.entrySet()) {
                put.addColumn(columnFamily.getBytes(), Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
            }
            table.put(put);
        } catch (Exception e){
            logger.error(e);
            logger.info("==========add error==========");
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //添加数据---按rowKey
    public void addByRowKey(BaseInfo baseInfo) {
        Table table = null;
        String rowKey = baseInfo.getRowKey();
        List<String> columnFamilyList = baseInfo.getColumnFamilyList();
        List<Map<String, Object>> columnsList = baseInfo.getColumnsList();

        try {
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (int i = 0; i < columnFamilyList.size(); i++ ) {
                for (Map.Entry<String, Object> entry : columnsList.get(i).entrySet()) {
                    put.addColumn(columnFamilyList.get(i).getBytes(),
                            Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
                }
            }
            table.put(put);
        } catch (Exception e){
            logger.error(e);
            logger.info("==========add error==========");
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //根据rowkey获取数据
    public Map<String, String> getAllValue(String rowKey) throws IllegalArgumentException {
        Table table = null;
        Map<String, String> resultMap = null;
        try {
            logger.info("==========getAllValue start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(series.getBytes());
            Result res = table.get(get);
            Map<byte[], byte[]> result = res.getFamilyMap(series.getBytes());
            Iterator<Entry<byte[], byte[]>> it = result.entrySet().iterator();
            resultMap = new HashMap<String, String>();
            while (it.hasNext()) {
                Entry<byte[], byte[]> entry = it.next();
                resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
            logger.info("==========getAllValue end==========");
        } catch (Exception e) {
            logger.error(e);
            logger.info("==========getAllValue error==========");
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultMap;
    }

    //根据rowkey和column获取数据
    public String getValueBySeries(String rowKey, String column) throws IllegalArgumentException {
        Table table = null;
        String resultStr = null;
        try {
            logger.info("==========getValueBySeries start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(series), Bytes.toBytes(column));
            Result res = table.get(get);
            byte[] result = res.getValue(Bytes.toBytes(series), Bytes.toBytes(column));
            resultStr = Bytes.toString(result);
            logger.info("==========getValueBySeries end==========");
        } catch (Exception e) {
            logger.error(e);
            logger.info("==========getValueBySeries error==========");
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultStr;
    }

    //根据table查询所有数据
    public ResultScanner  getValueByTable() {
        Map<String, String> resultMap = null;
        Table table = null;
        try {
            logger.info("==========getValueByTable start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            ResultScanner rs = table.getScanner(new Scan());
            logger.info("==========getValueByTable end==========");
            return rs;
        } catch (Exception e) {
            logger.error(e);
            logger.info("==========getValueByTable error==========");
        } finally {
            IOUtils.closeQuietly(table);
        }
        return null;
    }

    //根据rowKey查询数据
    public ResultScanner getValueByPreKey(String preRow){
        try {
            Scan s = new Scan();
            Table table = conn.getTable(TableName.valueOf(tableName));
            s.setFilter(new PrefixFilter(preRow.getBytes()));
            ResultScanner rs = table.getScanner(s);
            return rs;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    //删除表
    public void dropTable(String tableName) {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            logger.info("==========dropTable start==========");
            admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
            logger.info("==========dropTable end==========");
        } catch (Exception e) {
            logger.error(e);
            logger.info("==========dropTable error==========");
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }
}
