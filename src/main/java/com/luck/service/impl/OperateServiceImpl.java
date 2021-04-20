package com.luck.service.impl;
import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;


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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

public class OperateServiceImpl implements OperateService {
    private Logger logger = Logger.getLogger(this.getClass());

    private static String SERIES = "s";
    private static String TABLENAME = "truckTrace";
    private static Connection conn;


    public void init() {
        Configuration config = HBaseConfiguration.create();
        config.addResource("/src/main/resources/hbase-site.xml");
        try {
            logger.info("==========init start==========");
            conn = ConnectionFactory.createConnection(config);
            createTable(TABLENAME, SERIES);
            logger.info("===========init end===========");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(e);
            logger.info("==========init error==========");
        }
    }

    //创建表
    public void createTable(String tableName, String seriesStr) throws IllegalArgumentException, IOException {
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
            }
            logger.info("===========create end===========");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(e);
            logger.info("==========init error==========");
        }
        finally {
            IOUtils.closeQuietly(admin);
        }
    }

    //添加数据
    public void add(String rowKey, Map<String, Object> columns) throws IOException  {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(TABLENAME));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, Object> entry : columns.entrySet()) {
                put.addColumn(SERIES.getBytes(), Bytes.toBytes(entry.getKey()),
                        Bytes.toBytes("haha"));
            }
            table.put(put);
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //根据rowkey获取数据
    public Map<String, String> getAllValue(String rowKey) throws IllegalArgumentException, IOException {
        Table table = null;
        Map<String, String> resultMap = null;
        try {
            table = conn.getTable(TableName.valueOf(TABLENAME));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(SERIES.getBytes());
            Result res = table.get(get);
            Map<byte[], byte[]> result = res.getFamilyMap(SERIES.getBytes());
            Iterator<Entry<byte[], byte[]>> it = result.entrySet().iterator();
            resultMap = new HashMap<String, String>();
            while (it.hasNext()) {
                Entry<byte[], byte[]> entry = it.next();
                resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultMap;
    }

    //根据rowkey和column获取数据
    public String getValueBySeries(String rowKey, String column) throws IllegalArgumentException, IOException {
        Table table = null;
        String resultStr = null;
        try {
            table = conn.getTable(TableName.valueOf(TABLENAME));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addColumn(Bytes.toBytes(SERIES), Bytes.toBytes(column));
            Result res = table.get(get);
            byte[] result = res.getValue(Bytes.toBytes(SERIES), Bytes.toBytes(column));
            resultStr = Bytes.toString(result);
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultStr;
    }

    //根据table查询所有数据
    public void  getValueByTable() throws Exception {
        Map<String, String> resultMap = null;
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(TABLENAME));
            ResultScanner rs = table.getScanner(new Scan());
            for (Result r : rs) {
                System.out.println("get rowkey:" + new String(r.getRow()));
                for (KeyValue keyValue : r.raw()) {
                    System.out.println(
                            "row:" + new String(keyValue.getFamily()) + "====value:" + new String(keyValue.getValue()));
                }
            }
        } finally {
            IOUtils.closeQuietly(table);
        }
    }

    //删除表
    public void dropTable(String tableName) throws IOException {
        Admin admin = null;
        TableName table = TableName.valueOf(tableName);
        try {
            admin = conn.getAdmin();
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.deleteTable(table);
            }
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }
}