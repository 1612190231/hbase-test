package com.luck.service.impl;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.*;
import java.util.Map.Entry;


import com.luck.entity.BaseInfo;
import com.luck.service.OperateService;
import com.luck.utils.ByteUtil;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;

import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OperateServiceImpl implements OperateService {
    private Logger logger = LoggerFactory.getLogger(this.getClass());

    private String series;                  // 列族
    private String tableName;               // 表名
    private List<ServerName> serverNames;   // 服务器名
    private static Connection conn;

    public String getSeries() { return series; }

    public void setSeries(String series) { this.series = series; }

    public String getTableName() { return tableName; }

    public void setTableName(String tableName) { this.tableName = tableName; }

    public List<ServerName> getServerNames() {
        return serverNames;
    }

    public void setServerNames() throws IOException {
        Admin admin = conn.getAdmin();
        ClusterStatus clusterStatus = admin.getClusterStatus();
        Collection<ServerName> servers = clusterStatus.getServers();
        this.serverNames = new ArrayList<>(servers);
    }

    public void init() {
        Configuration config = HBaseConfiguration.create();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.addResource("/src/main/resources/hbase-site.xml");
        try {
            logger.info("==========init start==========");
            conn = ConnectionFactory.createConnection(config);

            Admin admin = conn.getAdmin();
            ClusterStatus clusterStatus = admin.getClusterStatus();
            Collection<ServerName> servers = clusterStatus.getServers();
            serverNames = new ArrayList<>(servers);

//            createTable(tableName, series);
            logger.info("===========init end===========");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(String.valueOf(e));
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
            logger.error(String.valueOf(e));
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
                logger.info(tableName + " table not Exists");
                HTableDescriptor descriptor = new HTableDescriptor(table);
                String[] series = seriesStr.split(",");
                for (String s : series) {
                    descriptor.addFamily(new HColumnDescriptor(s.getBytes()));
                }
                logger.info(Arrays.deepToString(startKey));
                admin.createTable(descriptor, startKey);
                logger.info("==========create success==========");
            }
            logger.info("===========create end===========");
        } catch (Exception e) {
            e.printStackTrace();
            logger.error(String.valueOf(e));
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
            logger.error(String.valueOf(e));
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
            logger.error(String.valueOf(e));
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
            logger.error(String.valueOf(e));
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
            logger.error(String.valueOf(e));
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
            logger.error(String.valueOf(e));
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

    //根据rowKey filter筛选数据
    public ResultScanner getValueByFilterKey(String keyRow) {
        try {
            Scan s = new Scan();
            Table table = conn.getTable(TableName.valueOf(tableName));
            Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(keyRow));
            s.setFilter(filter);
            return table.getScanner(s);
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
            logger.error(String.valueOf(e));
            logger.info("==========dropTable error==========");
        } finally {
            IOUtils.closeQuietly(admin);
        }
    }

    // 计算分区价值-单
    public long calculateRegionValue(Long rowKey) throws IOException {
        ByteUtil byteUtil = new ByteUtil();

        // 找region的起止key
        List<HRegionInfo> hRegionInfos = getRegions();
        HRegionInfo hRegionInfo = null;
        for(HRegionInfo item: hRegionInfos){
            // 根据rowKey确定在哪个region
            long startKey = byteUtil.convertBytesToLong(item.getStartKey());
            long endKey = byteUtil.convertBytesToLong(item.getEndKey());
            if (rowKey >= startKey && rowKey <= endKey){
                hRegionInfo = item;
                logger.info("HRegionInfo's RegionName: " + hRegionInfo.getRegionNameAsString());
            }
        }
        if(hRegionInfo == null){
            logger.error("找不到rowKey对应hRegionInfo");
            return -1;
        }

        // 找region的数据量、读命中次数
        Map<String, RegionLoad> regionLoadMap = getRegionLoad(serverNames);
        RegionLoad regionLoad = null;
        for (Map.Entry<String, RegionLoad> entry : regionLoadMap.entrySet()){
            if (Arrays.equals(hRegionInfo.getRegionName(),entry.getValue().getName())){
                regionLoad = entry.getValue();
                logger.info("RegionLoad's RegionName: " + regionLoad.getNameAsString());
            }
        }
        if (regionLoad == null){
            logger.error("找不到rowKey对应RegionLoad");
            return -1;
        }

        long startKey = byteUtil.convertBytesToLong(hRegionInfo.getStartKey());
        long regionSize = regionLoad.getStorefileSizeMB();
        long hitCount = regionLoad.getReadRequestsCount();
        return startKey + regionSize + hitCount;
    }

    // 计算分区价值-全
    public long calculateRegionValue(HRegionInfo hRegionInfo) throws IOException {
        ByteUtil byteUtil = new ByteUtil();
        // 找region的数据量、读命中次数
        Map<String, RegionLoad> regionLoadMap = getRegionLoad(serverNames);
        RegionLoad regionLoad = null;
        for (Map.Entry<String, RegionLoad> entry : regionLoadMap.entrySet()){
            if (Arrays.equals(hRegionInfo.getRegionName(),entry.getValue().getName())){
                regionLoad = entry.getValue();
                logger.info("RegionLoad's RegionName: " + regionLoad.getNameAsString());
            }
        }
        if (regionLoad == null){
            logger.error("找不到rowKey对应RegionLoad");
            return -1;
        }

        long startKey = byteUtil.convertBytesToLong(hRegionInfo.getStartKey());
        long regionSize = regionLoad.getStorefileSizeMB();
        long hitCount = regionLoad.getReadRequestsCount();
        return startKey + regionSize + hitCount;
    }

    // 查询regions
    public List<HRegionInfo> getRegions() throws IOException {
        List<HRegionInfo> regions;
        try (Admin admin = conn.getAdmin()) {
            regions = admin.getTableRegions(TableName.valueOf(tableName));
        }
        return regions;
    }

    // 查询regionLoads
    public Map<String, RegionLoad> getRegionLoad(List<ServerName> serverNames) throws IOException {
        Admin admin = conn.getAdmin();
        Map<String, RegionLoad> result = new HashMap<>();
        try {
            ClusterStatus clusterStatus = admin.getClusterStatus();
            for(ServerName serverName : serverNames) {
                ServerLoad serverLoad = clusterStatus.getLoad(serverName);
                Map<byte[], RegionLoad> regionLoads = serverLoad.getRegionsLoad();

                for(Map.Entry<byte[], RegionLoad> entry : regionLoads.entrySet()) {
                    String uniqueName = new String(entry.getKey()).split(",")[0];   //该region所属的table名;
                    RegionLoad regionLoad = entry.getValue();
                    if (uniqueName.equals(tableName)) {
                        result.put(new String(entry.getKey()), regionLoad);
                    }
                }
            }
        } finally {
            if(admin != null) {admin.close();}
        }
        return result;
    }
}
