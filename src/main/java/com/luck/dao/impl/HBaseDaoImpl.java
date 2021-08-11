package com.luck.dao.impl;

import com.luck.dao.HBaseDao;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.AggregationClient;
import org.apache.hadoop.hbase.client.coprocessor.LongColumnInterpreter;
import org.apache.hadoop.hbase.coprocessor.AggregateImplementation;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;
 
/**
 * @Author: gh
 * @Description: HBase表格数据查询，删除一条记录，新增一条记录，查询所有表格名称.
 */
public class HBaseDaoImpl implements HBaseDao {
 
    Connection conn = null;
    DataBaseDTO dataBaseDTO = null;
 
    public final String pk = "RowKey";
 
    //static Configuration conf = null;
    public HBaseDaoImpl(DataBaseDTO dto) {
        try {
            dataBaseDTO = dto;
            conn = connectToHBase();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
 
    public Connection getConn() {
        return conn;
    }
 
    public void setConn(Connection conn) {
        this.conn = conn;
    }
 
    /**
     * 关闭连接
     */
    public void close(){
        try{
            if(this.conn != null){
                conn.close();
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    public boolean connected(){
        if(getConn() == null){
            return false;
        }
        boolean tmp = isNamespaceExist(this.dataBaseDTO.getDbName());
        return !getConn().isClosed() && tmp;
    }
    /**
     * 安全通信：kerberos
     * zookeeper节点信息。
     *
     * @return true表示连接成功，false表示连接失败
     * @throws IOException
     */
    private org.apache.hadoop.hbase.client.Connection connectToHBase() {
        try {
            if (conn == null) {
                if (dataBaseDTO != null) {
                    Configuration conf = HBaseConfiguration.create();
                    String ip = dataBaseDTO.getIp();
                    String port = dataBaseDTO.getHost();
                    //NOTE1:如果ip不存在，会报错并retry30次：
                    // org.apache.zookeeper.ClientCnxn - Session 0x0 for server null,
                    // unexpected error, closing socket connection and attempting reconnect
                    //java.net.ConnectException: Connection refused: no further information
                    //org.apache.zookeeper.ClientCnxnSocketNIO - Ignoring exception during shutdown input
                    //java.nio.channels.ClosedChannelException: null
                    //NOTE2:如果ip存在，但不是zk的地址，会报错并retry：
                    // java.io.IOException: org.apache.zookeeper.KeeperException$NoNodeException:
                    // KeeperErrorCode = NoNode for /hbase/master
                    //TODO: 用户名、密码做校验
                    conf.set("hbase.zookeeper.quorum", ip+":"+port);
                    //conf.set("hbase.zookeeper.quorum", "zyb1:2181,zyb2:2181,zyb9:2181");
                    conf.setInt("zookeeper.recovery.retry", 0);
                    conf.setInt("zookeeper.session.timeout",30000);
                    conn =  ConnectionFactory.createConnection(conf);
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            return conn;
        }
    }
 
    /**
     * 获取表名全称： namespace:tableName
     * @param tableName
     * @return
     */
    protected String getFullTableName(String tableName){
        if(this.dataBaseDTO != null){
            String dbName = this.dataBaseDTO.getDbName();
            if(!StringUtil.isEmpty(dbName)&&!StringUtil.isEmpty(tableName)){
                //表名中不包含:
                if(tableName.indexOf(":")<0){
                    return new StringBuffer(dbName).append(":").append(tableName).toString();
                }
            }
        }
        return tableName;
    }
    public boolean isNamespaceExist(String nameSpace){
        boolean flag = false;
        Admin admin = null;
        try {
            if(!StringUtils.isEmpty(nameSpace)){
                admin = conn.getAdmin();
                //直接使用admin.getNamespaceDescriptor，可能会报错：
                // org.apache.hadoop.hbase.NamespaceNotFoundException
                NamespaceDescriptor[] nsds = admin.listNamespaceDescriptors();
                for (NamespaceDescriptor nsd : nsds) {
                    if(nsd.getName().equals(nameSpace)){
                        flag = true;
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return flag;
    }
 
    @Override
    public void createTable(String tableName, String[] columnFamilys) {
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            //拼接表名
            tableName = getFullTableName(tableName);
            TableName tblName = TableName.valueOf(tableName);
            if (admin.tableExists(tblName)) {
                System.err.println("此表已存在！");
            } else {
                HTableDescriptor htd = new HTableDescriptor(tblName);
                for (String columnFamily : columnFamilys) {
                    HColumnDescriptor hcd = new HColumnDescriptor(columnFamily);
                    htd.addFamily(hcd);
                }
                admin.createTable(htd);
                System.err.println("建表成功!");
            }
        } catch (MasterNotRunningException e) {
            e.printStackTrace();
        } catch (ZooKeeperConnectionException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
 
    @Override
    public int insert(String tableName, String rowKey, String family, String quailifer[], String value[]) {
        Table table = null;
        int rowCount = 0; //0 for nothing
        try {
            //拼接表名
            tableName = getFullTableName(tableName);
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                Put put = new Put(Bytes.toBytes(rowKey));
                // 批量添加
                for (int i = 0; i < quailifer.length; i++) {
                    String col = quailifer[i];
                    String val = value[i];
                    put.addColumn(Bytes.toBytes(family), Bytes.toBytes(col), Bytes.toBytes(val));
                }
                table.put(put);
                rowCount = 1; // row count of DML
                return rowCount;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rowCount;
    }
    @Override
    public List<Result> getAllRows(String tableName, Integer pageSize, Integer pageNum) {
        if (pageSize == null || pageSize < 1) {
            pageSize = 10;
        }
        if (pageNum == null || pageNum < 1) {
            pageNum = 1;
        }
        Table table = null;
        List<Result> list = new ArrayList<>();
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                //分页：设置每页的最大容量;获取startrow，第1页的startrow=null。
                ResultScanner scanner = getPageScanner(table, pageNum, pageSize);
                //返回的条数
                int resultSize = pageSize;
                for (Result rs : scanner) {
                    if(resultSize>0){
                        list.add(rs);
                        resultSize = resultSize -1;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
 
    /**
     * 分页过滤
     * 【注意：】该过滤器并不能保证返回的结果行数小于等于指定的页面行数，
     * 因为过滤器是分别作用到各个region server的，
     * 它只能保证当前region返回的结果行数不超过指定页面行数。
     * @param table 表对象
     * @param pageNum 当前页码
     * @param pageSize 当前页大小
     * @return
     * @throws Exception
     */
    private ResultScanner getPageScanner(Table table, int pageNum, int pageSize) throws Exception {
        Scan scan = new Scan();
        //第1页时，不需要设置startrow；其他页时，需要设置startrow
        Filter pageFilter = new PageFilter(pageSize);
        scan.setFilter(pageFilter);
        ResultScanner scanner = table.getScanner(scan);
        if (pageNum > 1) {
            for (int i = 1; i < pageNum; i++) {
                byte[] startRow = new byte[]{};
                for (Result result : scanner) {
                    startRow = result.getRow();
                }
                startRow = Bytes.add(startRow, new byte[]{0});
                scan.setStartRow(startRow);
                scanner = table.getScanner(scan);
            }
        }
        return scanner;
    }
 
    /**
     * 统计hbase中表的行数。方法：
     * 1.统计ResultScanner。效率和性能低下，不可取。
     * 2.使用协处理器中的“存储过程”--endpoint。
     * 3.Mapreduce任务。
     * @param tableName 表的名称
     * @return row的总行数
     */
    @Override
    public long totalCount(String tableName) {
        //拼接表名
        tableName = getFullTableName(tableName);
        return totalCount(tableName,new Scan());
    }
 
    /**
     * 统计hbase中表的行数。
     * @param tableName 表的名称
     * @param scan 扫描条件。
     * @return row的总行数
     */
    private long totalCount(String tableName,Scan scan){
        Admin admin = null;
        long rowCount = 0;
        long start = System.currentTimeMillis();
        System.out.println("start: "+new Date(start).toLocaleString());
        //使用协处理器时，（没有加载协处理器）报错：
        //org.apache.hadoop.hbase.exceptions.UnknownProtocolException:
        // No registered coprocessor service found for name AggregateService in region **.
        TableName tblName= null ;
        try {
            tblName = TableName.valueOf(tableName);
        } catch (Exception e) {
            e.printStackTrace();
        }
        if(tblName != null){
            try {
                //提前创建connection和conf
                Configuration conf = conn.getConfiguration();
                // conf.setBoolean("hbase.table.sanity.checks", false);
                admin = conn.getAdmin();
                //检查是不是已经添加过协处理器
                HTableDescriptor htd = admin.getTableDescriptor(tblName);
                String coprocessorName = AggregateImplementation.class.getName();
                boolean flag = htd.hasCoprocessor(coprocessorName);
                System.err.println("coprocessor: "+flag);
                if(!flag){
                    //先disable表，再添加协处理器。如果不disable，也会被自动disable。
                    asyncDisable(admin,tblName);
                    //添加协处理器
                    htd.addCoprocessor(coprocessorName);
                    //不修改表，协处理器就添加不到表属性中，会报错：UnknownProtocolException:
                    //No registered coprocessor service found for name AggregateService in region
                    admin.modifyTable(tblName, htd);
                    //enable table
                    asyncEnable(admin,tblName);
                }
                //使用协处理器进行row count
                AggregationClient ac = new AggregationClient(conf);
                //防止enable超时，表格依然是disable
                if(checkTableEnabled(admin, tblName)){
                    rowCount = ac.rowCount(tblName, new LongColumnInterpreter(), scan);
                }
                System.out.format("RowCount of %s : %s",tableName,rowCount);
                System.out.println();
            } catch (IOException e) {
                e.printStackTrace();
            }catch (Throwable e){
                e.printStackTrace();
            }finally{
                if(admin != null){
                    try {
                        asyncEnable(admin,tblName);
                        admin.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("end: "+new Date(end).toLocaleString());
        System.out.println("统计耗时："+(end-start)/1000+"s");
        return rowCount;
    }
    protected boolean checkTableEnabled(Admin admin,TableName tblName){
        try {
            return admin.isTableEnabled(tblName);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }
    private void asyncDisable(Admin admin,TableName tblName){
        try{
            if(admin.isTableEnabled(tblName)){
                System.err.println("==========disableTableAsync===========");
                admin.disableTableAsync(tblName);
                //如果没有禁用，就一直轮询
                long totalWait = 0;
                long maxWait = 30*1000;
                long sleepTime = 300;
                while(!admin.isTableDisabled(tblName)){
                    try {
                        Thread.sleep(sleepTime);
                        totalWait += sleepTime;
                        //最多等待30s
                        if(totalWait >= maxWait){
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    private void asyncEnable(Admin admin,TableName tblName){
        try{
            if(admin.isTableDisabled(tblName)){
                System.err.println("==========enableTableAsync===========");
                admin.enableTableAsync(tblName);
                //如果没有启用，就一直轮询
                long totalWait = 0;
                long maxWait = 30*1000;
                long sleepTime = 300;
                while(!admin.isTableEnabled(tblName)){
                    try {
                        Thread.sleep(sleepTime);
                        totalWait += sleepTime;
                        //最多等待30s
                        if(totalWait >= maxWait){
                            break;
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }
    @Override
    public Result getRowByKey(String tableName, String rowKey) {
        Table table = null;
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                Get get = new Get(Bytes.toBytes(rowKey));
                return table.get(get);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    @Override
    public List<Result> getRowsByKey(String tableName, String rowKeyLike) {
        Table table = null;
        List<Result> list = list = new ArrayList<>();
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKeyLike));
                Scan scan = new Scan();
                scan.setFilter(filter);
                ResultScanner scanner = table.getScanner(scan);
                for (Result rs : scanner) {
                    list.add(rs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
 
    @Override
    public List<Result> getRowsByKey(String tableName, String startRow, String stopRow) {
        Table table = null;
        List<Result> list = new ArrayList<>();
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                Scan scan = new Scan();
                scan.setStartRow(Bytes.toBytes(startRow));
                scan.setStopRow(Bytes.toBytes(stopRow));
                ResultScanner scanner = table.getScanner(scan);
                for (Result rs : scanner) {
                    list.add(rs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
 
    @Override
    public List<Result> getRowsByKey(String tableName, String rowKeyLike, String family, String cols[]) {
        Table table = null;
        List<Result> list = new ArrayList<>();
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKeyLike));
                Scan scan = new Scan();
                if (cols == null || cols.length < 1) {
                    scan.addFamily(Bytes.toBytes(family));
                } else {
                    for (int i = 0; i < cols.length; i++) {
                        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes(cols[i]));
                    }
                }
                scan.setFilter(filter);
                ResultScanner scanner = table.getScanner(scan);
                for (Result rs : scanner) {
                    list.add(rs);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
    @Override
    public int deleteRecords(String tableName, String rowKeyLike) {
        Table table = null;
        int rowCount = 0;
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                PrefixFilter filter = new PrefixFilter(Bytes.toBytes(rowKeyLike));
                Scan scan = new Scan();
                scan.setFilter(filter);
                ResultScanner scanner = table.getScanner(scan);
                List<Delete> list = new ArrayList<Delete>();
                for (Result rs : scanner) {
                    Delete del = new Delete(rs.getRow());
                    list.add(del);
                    rowCount ++;
                }
                table.delete(list);
                return rowCount;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return rowCount;
    }
    @Override
    public int deleteRecord(String tableName, String rowKey) {
        Table table = null;
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                //确认rowkey是否存在
                Get get = new Get(Bytes.toBytes(rowKey));
                Result result = table.get(get);
                List<Cell> cells = result.listCells();
                if(cells != null && cells.size() > 0){
                    //即使rowkey不存在，删除也不会报错。
                    Delete del = new Delete(Bytes.toBytes(rowKey));
                    table.delete(del);
                    return 1;
                }
            }
            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 0;
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    @Override
    public Map<String, List<String>> getNameSpaceTables() {
        Map<String, List<String>> result = new HashMap<>();
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            NamespaceDescriptor[] namespaces = admin.listNamespaceDescriptors();
            for (NamespaceDescriptor ns : namespaces) {
                String nsName = ns.getName();
                TableName[] tableNames = admin.listTableNamesByNamespace(nsName);
                List<String> tblList = new ArrayList<>();
                for (TableName name : tableNames) {
                    String tableName = name.getNameAsString();
                    //非default命名空间的表名有前缀 hbase:meta
                    tableName = tableName.contains(":") ? tableName.split(":")[1] : tableName;
                    tblList.add(tableName);
                    /*List<RegionInfo> regions = admin.getRegions(name);
                    for (RegionInfo region : regions) {
                        System.out.println("region id: "+region.getRegionId());
                    }
                    System.out.println("--------------------");*/
                }
                result.put(nsName, tblList);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return result;
    }
    @Override
    public List<String> getTablenamesOfDB(String nameSpace) {
        List<String> list = new ArrayList<>();
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            TableName[] tableNames = admin.listTableNamesByNamespace(nameSpace);
            for (TableName name : tableNames) {
                //判断table是不是enable的//过滤系统表
                if(checkTableEnabled(admin, name) && !name.isSystemTable()){
                    String tableName = name.getNameAsString();
                    //非default命名空间的表名有前缀 hbase:meta
                    tableName = tableName.contains(":") ? tableName.split(":")[1] : tableName;
                    list.add(tableName);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return list;
    }
    public int getTableStatus(String tableName){
        //拼接表名
        tableName = getFullTableName(tableName);
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            TableName tn = TableName.valueOf(tableName);
            boolean exist = admin.tableExists(tn);
            if(exist){
                //表存在,继续判断是否可用。
                boolean disabled = admin.isTableDisabled(tn);
                if(disabled){
                    //0:禁用
                    return 0;
                }else{
                    //1:可用
                    return 1;
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        //-1:表不存在
        return -1;
    }
    /**
     * 从HBase表的结果集提取出具体的字段值，即每行包括主键、列族、列名、列值。
     *
     * @param results
     * @param b 用来区分 是否返回geom 字段数据 true 是返回 false 是不返回
     */
    public Map<String,Object> extractResults(List<Result> results, boolean b) {
        Map<String,Object> rowDatas = new HashMap<>();
        List<Map<String,String>> data = new ArrayList<>();
        Set<String> fields = new HashSet<>();
        try {
            for (Result r : results) {
                //直接遍历Cell会有乱码
                //Cell[] cells = r.rawCells();
                Map<String, String> oneRowData = new HashMap<>();
                //TODO:
                oneRowData.put(pk, Bytes.toString(r.getRow()));
                NavigableMap<byte[], NavigableMap<byte[], byte[]>> maps = r.getNoVersionMap();
                //遍历列族
                NavigableSet<byte[]> keys = maps.navigableKeySet();
                for (byte[] key : keys) {
                    //TODO:
                    String columnFamily = Bytes.toString(key);
                    NavigableMap<byte[], byte[]> values = maps.get(key);
                    //遍历某个列族下的列
                    NavigableSet<byte[]> ks = values.navigableKeySet();
                    for (byte[] k : ks) {
                        //TODO:
                        String qualifierName = Bytes.toString(k);
                        String qualifierValue = Bytes.toString(values.get(k));
                        String fieldName = columnFamily + "." + qualifierName;
                        if(b){
                            oneRowData.put(fieldName, qualifierValue);
                        }else{
                            if(!"geom".equals(qualifierName)){
                                oneRowData.put(fieldName, qualifierValue);
                            }
                        }
                    }
                }
                fields.addAll(oneRowData.keySet());
                data.add(oneRowData);
            }
            rowDatas.put("data", data);
            rowDatas.put("fields", fields);
            rowDatas.put("pk", pk);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return rowDatas;
    }
    @Override
    public double usedRate() {
        double rate = 0.0;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            //获取region数据总量和总大小
            long regionSize = admin.getConfiguration()
                    .getLong("hbase.hregion.max.filesize", 10737418240L); //byte
            ClusterStatus clusterStatus = admin.getClusterStatus();
            int regionsCount = clusterStatus.getRegionsCount();
            final long totalRegionSizes = regionSize * regionsCount;
            Collection<ServerName> liveServers = clusterStatus.getServers();
            //获取storefile总大小(默认累加单位为MB)
            double totalHFileSizes = 0;
            for (ServerName liveServer : liveServers) {
                ServerLoad serverLoad = clusterStatus.getLoad(liveServer);
                if(serverLoad != null){
                    int storefileSizeInMB = serverLoad.getStorefileSizeInMB();
                    totalHFileSizes += (storefileSizeInMB);
                }
            }
            rate = (totalHFileSizes*1024*1024) / totalRegionSizes;
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return Double.parseDouble(String.format("%.2f",rate*100));
    }
    @Override
    public double storeSizeOfDB(SizeUnitEnum unit) {
        return storeSizeOfDB(null,unit);
    }
    @Override
    public double storeSizeOfDB(String nameSpace, SizeUnitEnum unit) {
        //获取storefile总大小(默认累加单位为MB)
        double totalHFileSizes = 0.00;
        Admin admin = null;
        try {
            admin = conn.getAdmin();
            ClusterStatus clusterStatus = admin.getClusterStatus();
            Collection<ServerName> liveServers = clusterStatus.getServers();
            if(nameSpace == null){
                //获取所有的storefile总大小
                for (ServerName server : liveServers) {
                    //当前活着的server
                    ServerLoad serverLoad = clusterStatus.getLoad(server);
                    if(serverLoad != null){
                        int storefileSizeInMB = serverLoad.getStorefileSizeInMB();
                        totalHFileSizes += (storefileSizeInMB);
                    }
                }
            }else if(isNamespaceExist(nameSpace)){
                //获取指定namespace下的所有表的storefile的大小
                TableName[] tableNames = admin.listTableNamesByNamespace(nameSpace);
                List<HRegionInfo> totalRegions = new ArrayList<>();
                for (TableName tableName : tableNames) {
                    //获取当前表分布在哪些region上
                    List<HRegionInfo> currentRegions = admin.getTableRegions(tableName);
                    totalRegions.addAll(currentRegions);
                }
                for (ServerName server : liveServers) {
                    //当前活着的server
                    ServerLoad serverLoad = clusterStatus.getLoad(server);
                    //当前server中所有的regions
                    Map<byte[], RegionLoad> regionLoads = serverLoad.getRegionsLoad();
                    //查看哪些regions是属于当前namespace中的表的
                    for (HRegionInfo tableRegion : totalRegions) {
                        //获取以上各个region中storefile的大小
                        RegionLoad rl = regionLoads.get(tableRegion.getRegionName());
                        if(rl != null){
                            totalHFileSizes += (rl.getStorefileSizeMB());
                        }
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return FileSizeUtil.valueOf(totalHFileSizes*1024*1024, unit);
    }
    @Override
    public double storeSizeOfTbl(String nameSpace,String[] tables, SizeUnitEnum unit) {
        //获取storefile总大小(默认累加单位为MB)
        double totalHFileSizes = 0.00;
        Admin admin =null;
        try {
            boolean flag = isNamespaceExist(nameSpace);
            if(flag && tables != null){
                admin = conn.getAdmin();
                ClusterStatus clusterStatus = admin.getClusterStatus();
                Collection<ServerName> liveServers = clusterStatus.getServers();
                List<HRegionInfo> totalRegions = new ArrayList<>();
                for (String table : tables) {
                    //获取当前表分布在哪些region上
                    String fullTableName = nameSpace+":"+table;
                    if(table.contains(":")){
                        fullTableName = table;
                    }
                    TableName currentTable = TableName.valueOf(fullTableName);
                    List<HRegionInfo> currentRegions = admin.getTableRegions(currentTable);
                    totalRegions.addAll(currentRegions);
                }
                for (ServerName server : liveServers) {
                    //当前活着的server
                    ServerLoad serverLoad = clusterStatus.getLoad(server);
                    //当前server中所有的regions
                    Map<byte[], RegionLoad> regionLoads = serverLoad.getRegionsLoad();
                    //查看哪些regions是属于当前namespace中的表的
                    for (HRegionInfo tableRegion : totalRegions) {
                        //获取以上各个region中storefile的大小
                        RegionLoad rl = regionLoads.get(tableRegion.getRegionName());
                        if(rl != null){
                            totalHFileSizes += (rl.getStorefileSizeMB());
                        }
                    }
                }
            }
        } catch (Exception e) {
            System.out.println("===================="+e.getMessage());
            e.printStackTrace();
        }finally{
            if(admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return FileSizeUtil.valueOf(totalHFileSizes*1024*1024, unit);
    }
    @Override
    public int countTables(String nameSpace) {
        List<String> list = getTablenamesOfDB(nameSpace);
        return list.size();
    }
    @Override
    public List<String> getTables(String nameSpace) {
        List<String> list = getTablenamesOfDB(nameSpace);
        return list;
    }
    @Override
    public List<Result> queryByRandomField(String tableName, String familyName,String qualifier,
                                           String fieldValue,Integer pageSize,Integer pageNum) {
        if (pageSize == null || pageSize < 1) {
            pageSize = 10;
        }
        if (pageNum == null || pageNum < 1) {
            pageNum = 1;
        }
        Table table = null;
        List<Result> list = new ArrayList<>();
        //拼接表名
        tableName = getFullTableName(tableName);
        try {
            TableName tblName = TableName.valueOf(tableName);
            if(checkTableEnabled(conn.getAdmin(),tblName)){
                table = conn.getTable(tblName);
                Scan scan = new Scan();
                Filter columnFilter = null;
                if(pk.equals(qualifier)){
                    //过滤1：模糊匹配rowkey的值，找到所有row
                    columnFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(fieldValue));
                }else{
                    //过滤2：模糊匹配某个字段的值（非rowkey）
                    columnFilter = new SingleColumnValueFilter(
                            Bytes.toBytes(familyName),
                            Bytes.toBytes(qualifier),
                            CompareFilter.CompareOp.EQUAL,
                            new SubstringComparator(fieldValue));
                }
                //过滤3：页面大小，为分页准备
                Filter pageFilter = new PageFilter(pageSize);
                //添加过滤器
                List<Filter> filters = new ArrayList<>();
                filters.add(columnFilter);
                filters.add(pageFilter);
                scan.setFilter(new FilterList(filters));
                //查询
                ResultScanner scanner = table.getScanner(scan);
                if (pageNum > 1) {
                    for (int i = 1; i < pageNum; i++) {
                        byte[] startRow = new byte[]{};
                        for (Result result : scanner) {
                            startRow = result.getRow();
                        }
                        startRow = Bytes.add(startRow, new byte[]{0});
                        scan.setStartRow(startRow);
                        scanner = table.getScanner(scan);
                    }
                }
                int resultSize = pageSize;
                for (Result rs : scanner) {
                    if(resultSize>0){
                        list.add(rs);
                        resultSize = resultSize -1;
                    }
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if(table != null){
                    table.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return list;
    }
 
    @Override
    public long totalCountOfFuzzyQuery(String tableName,String familyName,
                                       String qualifier,String fieldValue) {
        //拼接表名
        tableName = getFullTableName(tableName);
        //添加scan条件
        Scan scan = new Scan();
        Filter columnFilter = null;
        if(pk.equals(qualifier)){
            //过滤1：模糊匹配rowkey的值，找到所有row
            columnFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(fieldValue));
        }else{
            //过滤2：模糊匹配某个字段的值（非rowkey）
            columnFilter = new SingleColumnValueFilter(
                    Bytes.toBytes(familyName),
                    Bytes.toBytes(qualifier),
                    CompareFilter.CompareOp.EQUAL,
                    new SubstringComparator(fieldValue));
        }
        //添加过滤器
        scan.setFilter(columnFilter);
        //使用coprocessor计数
        return totalCount(tableName, scan);
    }
 
    @Override
    public Map<String, String> getColumnNames(String tableName) {
        return getColumnNames(tableName,null,null);
    }
    private Map<String, String> getColumnNames(String tableName,Integer pageSize,Integer pageNum){
        List<Result> someRows = getAllRows(tableName, pageSize, pageNum);
        Map<String, Object> map = extractResults(someRows, true);
        Object fields = map.get("fields");
        Map<String, String> columnNames = new HashMap<>();
        if(fields != null){
            //有列的名称
            Set<String> set = (Set<String>)fields;
            for (String field : set) {
                columnNames.put(field, "");
            }
        }
        return columnNames;
    }
}