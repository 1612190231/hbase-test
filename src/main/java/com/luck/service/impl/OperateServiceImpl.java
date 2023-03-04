package com.luck.service.impl;

import com.luck.entity.BaseInfo;
import com.luck.entity.THRegionInfo;
import com.luck.entity.THServerInfo;
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

import javax.swing.plaf.synth.Region;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

public class OperateServiceImpl implements OperateService {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    private String series;                  // 列族
    private String tableName;               // 表名
    private List<ServerName> serverNames;   // 服务器名
    private static Connection conn;
    private BufferedMutator.ExceptionListener listener; // 报错信息

    public String getSeries() { return series; }

    public void setSeries(String series) { this.series = series; }

    public String getTableName() { return tableName; }

    public void setTableName(String tableName) { this.tableName = tableName; }

    public List<ServerName> getServerNames() {
        return serverNames;
    }

    public ServerName getServerName(String serverStr) {
        for (ServerName serverName: serverNames) {
            if (serverName.getServerName().startsWith(serverStr)) {
                return serverName;
            }
        }
        return null;
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
            listener = (e, mutator) -> {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            };

            Admin admin = conn.getAdmin();
            ClusterStatus clusterStatus = admin.getClusterStatus();
            Collection<ServerName> servers = clusterStatus.getServers();
            this.serverNames = new ArrayList<>(servers);

//            createTable(tableName, series);
            logger.info("===========init end===========");
        } catch (IOException e) {
            e.printStackTrace();
            logger.error(String.valueOf(e));
            logger.info("==========init error==========");
        }
    }

    public void init(String tableName, String series) {
        this.tableName = tableName;
        this.series = series;

        Configuration config = HBaseConfiguration.create();
        config.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        config.addResource("/src/main/resources/hbase-site.xml");
        try {
            logger.info("==========init start==========");
            conn = ConnectionFactory.createConnection(config);
            listener = (e, mutator) -> {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    logger.info("Failed to sent put " + e.getRow(i) + ".");
                }
            };

            Admin admin = conn.getAdmin();
            ClusterStatus clusterStatus = admin.getClusterStatus();
            Collection<ServerName> servers = clusterStatus.getServers();
            this.serverNames = new ArrayList<>(servers);

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
            logger.info("==========add start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            Put put = new Put(Bytes.toBytes(rowKey));
            for (Entry<String, Object> entry : columns.entrySet()) {
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
    public void addByMutator(List<BaseInfo> baseInfos) {
        BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(tableName)).listener(listener);
        params.writeBufferSize(4*1023*1024);
        try {
            int count = 0;
            ArrayList<Put> puts = new ArrayList<Put>();
            BufferedMutator mutator = conn.getBufferedMutator(params);
            for (BaseInfo baseInfo : baseInfos) {
                String rowKey = baseInfo.getRowKey();
                List<String> columnFamilyList = baseInfo.getColumnFamilyList();
                List<Map<String, Object>> columnsList = baseInfo.getColumnsList();
                Put put = new Put(Bytes.toBytes(rowKey));
                for (int i = 0; i < columnFamilyList.size(); i++) {
                    for (Entry<String, Object> entry : columnsList.get(i).entrySet()) {
                        count++;
                        put.addColumn(columnFamilyList.get(i).getBytes(),
                                Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));
                        puts.add(put);
                        if (count % 10000 == 0) {
                            mutator.mutate(puts);
                            puts.clear();
                        }
                    }
                }
            }
            mutator.mutate(puts);
            mutator.close();
            puts.clear();
        } catch(Exception e){
            logger.error(String.valueOf(e));
            logger.info("==========add error==========");
        } finally{
            baseInfos.clear();
        }
    }

    //添加数据---按rowKey list
    public void addByListPut(List<BaseInfo> baseInfos) {
        Table table = null;
        try {
            table = conn.getTable(TableName.valueOf(tableName));
            List<Put> puts = new ArrayList<>();
            for (BaseInfo baseInfo : baseInfos) {
                String rowKey = baseInfo.getRowKey();
                List<String> columnFamilyList = baseInfo.getColumnFamilyList();
                List<Map<String, Object>> columnsList = baseInfo.getColumnsList();
                Put put = new Put(Bytes.toBytes(rowKey));
                for (int i = 0; i < columnFamilyList.size(); i++) {
                    for (Entry<String, Object> entry : columnsList.get(i).entrySet()) {
                        put.addColumn(columnFamilyList.get(i).getBytes(),
                                Bytes.toBytes(entry.getKey()), Bytes.toBytes((long)entry.getValue()));
                    }
                }
                puts.add(put);
            }

            table.put(puts);
        } catch(Exception e){
            logger.error(String.valueOf(e));
            logger.info("==========add error==========");
        } finally{
            IOUtils.closeQuietly(table);
        }
    }

    //根据rowkey获取数据
    public List<RegionInfo> getRegions() throws IllegalArgumentException {
        List<RegionInfo> regionInfos = null;
        try {
            Admin admin = conn.getAdmin();
            regionInfos = admin.getRegions(TableName.valueOf("test"));
            logger.info("==========getRegions success==========");
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            logger.info("==========getRegions error==========");
        }
        return regionInfos;
    }

    //根据rowkey获取数据
    public Map<String, String> getByRowKey(String rowKey) throws IllegalArgumentException {
        Table table = null;
        Map<String, String> resultMap = new HashMap<>();
        try {
            logger.info("==========getAllValue start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            Get get = new Get(Bytes.toBytes(rowKey));
            get.addFamily(series.getBytes());
            Result res = table.get(get);
            Map<byte[], byte[]> result = res.getFamilyMap(series.getBytes());
//            logger.info(String.valueOf(result.size()));
            for (Entry<byte[], byte[]> entry : result.entrySet()) {
                resultMap.put(Bytes.toString(entry.getKey()), Bytes.toString(entry.getValue()));
//                logger.info(Bytes.toString(entry.getKey()));
//                logger.info(Bytes.toString(entry.getValue()));
            }
//            if (resultMap.containsKey("rowKey")) {
//                logger.info(resultMap.get("rowKey"));
//            }
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
    public String getBySeries(String rowKey, String column) throws IllegalArgumentException {
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
            logger.error("==========getValueBySeries error==========");
        } finally {
            IOUtils.closeQuietly(table);
        }
        return resultStr;
    }

    //根据table查询所有数据
    public ResultScanner  getByTable() {
        Table table = null;
        ResultScanner rs = null;
        try {
//            logger.info("==========getValueByTable start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            rs = table.getScanner(new Scan());
        } catch (Exception e) {
            logger.error(String.valueOf(e));
            logger.error("==========getValueByTable error==========");
        } finally {
            IOUtils.closeQuietly(table);
//            logger.info("==========getValueByTable end==========");
        }
        return rs;
    }

    //根据filter筛选数据
    public ResultScanner getByFilter(Filter filter){
        Table table = null;
        ResultScanner rs = null;
        try {
//            logger.info("==========getByFilter start==========");
            table = conn.getTable(TableName.valueOf(tableName));
            Scan scan = new Scan();
            scan.setFilter(filter);
            rs = table.getScanner(scan);
        } catch (IOException e) {
            logger.error(String.valueOf(e));
            logger.error("==========getByFilter error==========");
        } finally {
            IOUtils.closeQuietly(table);
//            logger.info("==========getByFilter end==========");
        }
        return rs;
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

    // 获取分区数据
    public List<THServerInfo> getRegionsStatus(String[] res) throws IOException {
        List<THServerInfo> thServerInfos = new ArrayList<>();
        Admin admin = conn.getAdmin();
        Map<String, List> keyMap = new HashMap<>();
        for (ServerName serverName: serverNames){
            logger.info(String.valueOf(serverName));

            THServerInfo thServerInfo = new THServerInfo();
            thServerInfo.setServerName(serverName);
            thServerInfo.setTableName(tableName);

            // 查节点分区数量
            List<RegionInfo> regionInfos = admin.getRegions(serverName);
            long regionCount = 0;
            for (RegionInfo regionInfo: regionInfos){
                String uniqueName = new String(regionInfo.getRegionName()).split(",")[0];
                if (uniqueName.equals(tableName)) {
                    String tmpStartKey = Bytes.toString(regionInfo.getStartKey());
                    String tmpEndKey = Bytes.toString(regionInfo.getEndKey());
                    String startKey = tmpStartKey.equals("") ? "00505000000000" : tmpStartKey.substring(0, 14);
                    String endKey = tmpEndKey.equals("") ? "00847000000000" : tmpEndKey.substring(0, 14);

                    String regionName = Bytes.toString(regionInfo.getRegionName());

                    logger.info(Bytes.toString(regionInfo.getRegionName()));
                    keyMap.put(regionName, new ArrayList<Object>() {{
                        add(startKey);
                        add(endKey);
                    }});
                    regionCount ++;

                    long sum = 0;
                    for (String rowKey: res) {
//                        logger.info("======================================");
//                        logger.info(rowKey);
//                        logger.info(startKey.substring(0, 14));
//                        logger.info(endKey.substring(0, 14));
                        if (rowKey.compareTo(startKey) > 0 && rowKey.compareTo(endKey) < 0) {
                            sum++;
                        }
                    }
                    keyMap.get(regionName).add(sum);
                    logger.info(String.valueOf(sum));
                }
            }
            logger.info("--------------------------------------------");
            thServerInfo.setRegionCount(regionCount);   // 分区数量

            // 查分区数据量、分区查询命中次数、server总分区数据量
            ClusterMetrics clusterMetrics = admin.getClusterMetrics();
            Map<ServerName, ServerMetrics> serverMetricsMap = clusterMetrics.getLiveServerMetrics();
            ServerMetrics serverMetrics = serverMetricsMap.get(serverName);
            Map<byte[], RegionMetrics> regionMetrics = serverMetrics.getRegionMetrics();
            List<THRegionInfo> thRegionInfos = new ArrayList<>();
            long serverHitCount = 0;
            long serverRegionSize = 0;
            for(Entry<byte[], RegionMetrics> entry : regionMetrics.entrySet()) {
                String regionName = new String(entry.getKey());
                String uniqueName = new String(entry.getKey()).split(",")[0];   // 该region所属的table名;
                RegionMetrics regionLoad = entry.getValue();
                if (uniqueName.equals(tableName)) {
                    THRegionInfo thRegionInfo = new THRegionInfo();
                    thRegionInfo.setRegionName(regionName);
                    thRegionInfo.setTableNmae(tableName);

                    long regionHitCount = (long) keyMap.get(regionName).get(2);
                    long regionSize = Math.round(regionLoad.getStoreFileSize().get(Size.Unit.MEGABYTE));

                    thRegionInfo.setRegionSize(regionSize);     // 分区数据规模
                    thRegionInfo.setHitCount(regionHitCount);   // 分区查询命中次数
//                    logger.info(regionName);
                    thRegionInfo.setStartKey((String) keyMap.get(regionName).get(0));
                    thRegionInfo.setEndKey((String) keyMap.get(regionName).get(1));

                    thRegionInfos.add(thRegionInfo);

                    serverRegionSize += regionSize;
                    serverHitCount += regionHitCount;
                }
            }

            thServerInfo.setRegionInfos(thRegionInfos);
            thServerInfo.setSumHitCount(serverHitCount);    // server总查询命中次数
            thServerInfo.setSumRegionSize(serverRegionSize);
            thServerInfos.add(thServerInfo);
        }
        return thServerInfos;
    }

    // 手动分区
    public boolean splitRegion(String regionName) throws IOException {
        Admin admin = conn.getAdmin();
        try {
            admin.splitRegionAsync(Bytes.toBytes(regionName));
        } catch (Exception e){
            logger.error("split region error...");
            logger.error(e.toString());
            return false;
        }
        return true;
    }

    // 手动合区
    public void mergeRegion(String regionName1, String regionName2) throws IOException {
        Admin admin = conn.getAdmin();
        byte[][] nameofRegionsToMerge = new byte[][]{Bytes.toBytes(regionName1), Bytes.toBytes(regionName2)};
        try {
            admin.mergeRegionsAsync(nameofRegionsToMerge, false);
        } catch (Exception e) {
            logger.error("merge region error...");
            logger.error(e.toString());
        }
    }

    // 手动移动region
    public void moveRegion(String regionName, ServerName serverName) throws IOException {
        Admin admin = conn.getAdmin();
        try {
            admin.move(Bytes.toBytes(regionName), serverName);
        } catch (Exception e) {
            logger.error("move region error...");
            logger.error(e.toString());
        }
    }

    // 清空表
    public void truncateTable() throws IOException {
        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf(tableName);
        try {
            if (admin.tableExists(table)) {
                admin.disableTable(table);
                admin.truncateTable(table, false);
            }
        } catch (Exception e) {
            logger.error("truncate table error...");
            logger.error(e.toString());
        }
    }

    // 平衡表
    public void balanceTable() throws IOException {
        Admin admin = conn.getAdmin();
        TableName table = TableName.valueOf(tableName);
        try {
            admin.balance();
        } catch (Exception e) {
            logger.error("balance table error...");
            logger.error(e.toString());
        }
    }
}
