package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.entity.KeyInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.OperateService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.ByteUtil;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class MineInsert {

    public static void main(String[] args) throws MalformedURLException, ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();
        ByteUtil byteUtil = new ByteUtil();

        long startTime=System.currentTimeMillis(); //获取开始时间

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
//        URL url = new URL("file:////C:\\Users\\user\\Desktop\\code\\hbase-test\\src\\main\\resources\\-1010.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
        URL url = new URL(args[0]);
        List<TrajectoryInfo> trajectoryInfos = hbaseShardService.getTrajectoryInfos(url);
        logUtil.print("load url: " + url);

        // 构造索引
        trajectoryInfos = hbaseShardService.dimensionReduction(trajectoryInfos);

        // 添加数据集---按rowKey
        List<BaseInfo> baseInfos = new ArrayList<>();
        List<String> shardRowKeys = new ArrayList<>();
        for (TrajectoryInfo trajectoryInfo : trajectoryInfos) {
            //            logUtil.prepareLog(list);
            if (trajectoryInfo.getKeyRange() == null || trajectoryInfo.getKeyTime() == null){
                continue;
            }
            BaseInfo baseInfo = new BaseInfo();
            List<String> columnFamilyList = new ArrayList<>();
            List<Map<String, Object>> columnsList = new ArrayList<>();

            int ran = (int) (Math.random() * 100);
            String numHash = String.format("%03d", ran);
            String keySum = trajectoryInfo.getKeyTime() + trajectoryInfo.getKeyRange();
            String rowKey = keySum + trajectoryInfo.getVehicleNo();
//            String rowKey = numHash + keySum + trajectoryInfo.getVehicleNo();
            shardRowKeys.add(keySum);

            try {
                HashMap<String, Object > myMap  = new HashMap<String, Object>(){{
                    put("vehicleNo",trajectoryInfo.getVehicleNo());
                    put("minLon",trajectoryInfo.getMinLon());
                    put("minLat",trajectoryInfo.getMinLat());
                    put("maxLon",trajectoryInfo.getMaxLon());
                    put("maxLat",trajectoryInfo.getMaxLat());
                    put("minTime",trajectoryInfo.getMinTime());
                    put("maxTime",trajectoryInfo.getMaxTime());
                    put("points",trajectoryInfo.toString()); // 获取点列表
                }};

                columnFamilyList.add("data");
                columnsList.add(myMap);
            } catch (Exception e) {
                logUtil.print("插入报错rowKey:" + rowKey);
            }

            baseInfo.setRowKey(rowKey);
            baseInfo.setColumnFamilyList(columnFamilyList);
            baseInfo.setColumnsList(columnsList);
            baseInfos.add(baseInfo);
        }

        //开始hbase操作
        //初始化
        OperateService operateService = new OperateServiceImpl();
        operateService.setSeries("data");
        operateService.setTableName("track_mine");
        operateService.init();

        // 预分区
//        List<Object> rowKeyList = new ArrayList<>();
//        shardRowKeys.forEach(p -> {if (!rowKeyList.contains(p)) {rowKeyList.add(p);}});
//        byte[][] startKey = getSplitKeys(shardRowKeys);
        // 打印分布
//        printPartition(rowKeys);

        // 创建表
        String series = operateService.getSeries();
        String tableName = operateService.getTableName();
        operateService.createTable(tableName,series);
//        operateService.createTable(tableName,series, startKey);
//        System.out.println(Arrays.deepToString(startKey));

        // 插入数据
        operateService.addByMutator(baseInfos);
        long endTime=System.currentTimeMillis(); //获取结束时间
        logUtil.runTimeLog("addAll", endTime, startTime);
    }

    private static byte[][] getSplitKeys(List<String> shardRowKeys) {
        Map<String, Integer> treeMap = new TreeMap<>();
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for(String keyInfo: shardRowKeys){
            treeMap.put(keyInfo, treeMap.getOrDefault(keyInfo, 0) + 1);
            byte[] byteKey = keyInfo.getBytes(StandardCharsets.UTF_8);
            rows.add(byteKey);
        }
        byte[][] splitKeys = new byte[rows.size()][];
        Iterator<byte[]> rowKeyIter = rows.iterator();
        int i = 0;
        while (rowKeyIter.hasNext()) {
            byte[] tempRow = rowKeyIter.next();
            rowKeyIter.remove();
            splitKeys[i++] = tempRow;
        }
        return splitKeys;
    }

    private static void printPartition(List<KeyInfo> rowKeys){
        // 打印key分布
        Map<String, Integer> keyTimeMap = new HashMap<String, Integer>();
        Map<String, Integer> keyRangeMap = new HashMap<String, Integer>();
        Map<String, Integer> keyMap = new HashMap<String, Integer>();
        for(KeyInfo keyInfo: rowKeys){
            // 整体
            if (!keyMap.containsKey("" + keyInfo.getTimeKey() + '-' + keyInfo.getRangeKey())){
                keyMap.put("" + keyInfo.getTimeKey() + '-' + keyInfo.getRangeKey(), 1);
            }
            else{
                String key = "" + keyInfo.getTimeKey() + '-' + keyInfo.getRangeKey();
                keyMap.put(key, keyMap.get(key) + 1);
            }

            // time
            if (!keyTimeMap.containsKey(keyInfo.getTimeKey())){
                keyTimeMap.put(keyInfo.getTimeKey(), 1);
            }
            else{
                String key = keyInfo.getTimeKey();
                keyTimeMap.put(key, keyTimeMap.get(key) + 1);
            }

            // range
            if (!keyRangeMap.containsKey(keyInfo.getRangeKey())){
                keyRangeMap.put(keyInfo.getRangeKey(), 1);
            }
            else{
                String key = keyInfo.getRangeKey();
                keyRangeMap.put(key, keyRangeMap.get(key) + 1);
            }
        }
        // all
        System.out.println("keyMap start...");
        Map<String, Integer> result2 = new LinkedHashMap<String, Integer>();
        keyMap.entrySet()
                .stream().sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x -> result2.put(x.getKey(), x.getValue()));
        result2.forEach((key, value) -> System.out.println(key + "," + value));

        // time
        System.out.println("keyTimeMap start...");
        keyTimeMap.forEach((key, value) -> System.out.println(key + ": " + value));

        // range
        System.out.println("keyRangeMap start...");
        Map<String, Integer> result1 = new LinkedHashMap<String, Integer>();
        keyRangeMap.entrySet()
                .stream().sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x -> result1.put(x.getKey(), x.getValue()));
        result1.forEach((key, value) -> System.out.println(key + "," + value));
    }
}
