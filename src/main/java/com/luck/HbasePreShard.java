package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.entity.KeyInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.index.splitkey.impl.HashSplistKeysCalculator;
import com.luck.service.HbaseShardService;
import com.luck.service.OperateService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.ByteUtil;
import com.luck.utils.LogUtil;
import org.apache.commons.collections.map.HashedMap;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.Collator;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class HbasePreShard {

    public static void main(String[] args) throws MalformedURLException, ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();
        ByteUtil byteUtil = new ByteUtil();

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
//        String str = "data/test.csv";
//        URL url = new URL("file:////C:\\Users\\user\\Desktop\\code\\hbase-test\\src\\main\\resources\\data\\test.csv");
        URL url = new URL("file:////C:\\Users\\13908\\Desktop\\code\\hbase-test\\src\\main\\resources\\data\\-0620.csv");
//        URL url = new URL("file:////root/hbase/data/test.csv");
        List<TrajectoryInfo> trajectoryInfos = hbaseShardService.getTrajectoryInfos(url);
//        List<OrgVo> orgList = new ArrayList();
//        trajectoryInfos = trajectoryInfos.stream().filter(TrajectoryInfo -> TrajectoryInfo.getPointInfos().size() > 1).collect(Collectors.toList());
//        Collections.sort(trajectoryInfos, new Comparator<TrajectoryInfo>() {
//            public int compare(TrajectoryInfo o1, TrajectoryInfo o2) {
//                //倒序排列的话，两参数互换就行
//                return Collator.getInstance(Locale.CHINA).compare(o1.getVehicleNo(), o2.getVehicleNo());
//            }
//        });

        // 构造索引
        trajectoryInfos = hbaseShardService.dimensionReduction(trajectoryInfos);

        // 导出keys
//        List<String> keys = new ArrayList<String>();
//        for(TrajectoryInfo trajectoryInfo: trajectoryInfos){
//            String key = trajectoryInfo.getVehicleNo() + ',' + trajectoryInfo.getKeyTime() + ',' + trajectoryInfo.getKeyRange();
//            keys.add(key);
//        }
//        File file = new File("src/main/resources/keys" + str);
//        CsvUtil.exportCsv(file, keys);

        // 添加数据集---按rowKey
        List<BaseInfo> baseInfos = new ArrayList<>();
        List<KeyInfo> shardRowKeys = new ArrayList<>();
        for (TrajectoryInfo trajectoryInfo : trajectoryInfos) {
            //            logUtil.prepareLog(list);
            if (trajectoryInfo.getKeyRange() == null || trajectoryInfo.getKeyTime() == null){
                continue;
            }
            BaseInfo baseInfo = new BaseInfo();
            List<String> columnFamilyList = new ArrayList<>();
            List<Map<String, Object>> columnsList = new ArrayList<>();
            KeyInfo keyInfo = new KeyInfo(trajectoryInfo.getKeyTime(), trajectoryInfo.getKeyRange());
            String rowKey = Arrays.toString(keyInfo.toBytes()) + '-' + trajectoryInfo.getVehicleNo();
            shardRowKeys.add(keyInfo);
//            int rowKey = (int)(Math.random() * 50) + 1;
//          System.out.print(list.get(j));

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
//                hbaseShardInsert.error("插入报错rowKey:" + rowKey);
            }

            baseInfo.setRowKey(rowKey);
            baseInfo.setColumnFamilyList(columnFamilyList);
            baseInfo.setColumnsList(columnsList);
            baseInfos.add(baseInfo);
        }

        // 开始打印分布
//        printPartition(rowKeys);

        //开始hbase操作
        //初始化
//        OperateService operateService = new OperateServiceImpl();
//        operateService.setSeries("data");
//        operateService.setTableName("hbase_shard");
//        operateService.init();
//
//        // 创建表
//        String series = operateService.getSeries();
//        String tableName = operateService.getTableName();
        // 预分区
        List rowKeyList = new ArrayList<>();
        shardRowKeys.stream().forEach(p -> {if (!rowKeyList.contains(p)) {rowKeyList.add(p);}});
        byte[][] startKey = getSplitKeys(shardRowKeys);
//        operateService.createTable(tableName,series, startKey);
//        System.out.println(startKey);

        // 插入数据
//        long startTime=System.currentTimeMillis(); //获取开始时间
//        for(BaseInfo baseInfo: baseInfos){
//            operateService.addByRowKey(baseInfo);
//        }
//        long endTime=System.currentTimeMillis(); //获取结束时间
//        logUtil.runTimeLog("addAll", endTime, startTime);

//        //查看表中所有数据
//        long startTime2=System.currentTimeMillis(); //获取开始时间
//        ResultScanner rs = operateService.getValueByTable();
//        long endTime2=System.currentTimeMillis(); //获取结束时间
//        logUtil.runTimeLog("getValueByTable", endTime2, startTime2);
//        logUtil.getValueByTable(rs);
    }

    private static byte[][] getSplitKeys(List<KeyInfo> shardRowKeys) {
        byte[][] splitKeys = new byte[shardRowKeys.size()][];
        TreeSet<byte[]> rows = new TreeSet<byte[]>(Bytes.BYTES_COMPARATOR);//升序排序
        for(KeyInfo keyInfo: shardRowKeys){
            byte[] byteKey = keyInfo.toBytes();
            rows.add(byteKey);
        }
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
        Map<Long, Integer> keyTimeMap = new HashMap<Long, Integer>();
        Map<Long, Integer> keyRangeMap = new HashMap<Long, Integer>();
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
                Long key = keyInfo.getTimeKey();
                keyTimeMap.put(key, keyTimeMap.get(key) + 1);
            }

            // range
            if (!keyRangeMap.containsKey(keyInfo.getRangeKey())){
                keyRangeMap.put(keyInfo.getRangeKey(), 1);
            }
            else{
                Long key = keyInfo.getRangeKey();
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
        Map<Long, Integer> result1 = new LinkedHashMap<Long, Integer>();
        keyRangeMap.entrySet()
                .stream().sorted(Map.Entry.comparingByKey())
                .forEachOrdered(x -> result1.put(x.getKey(), x.getValue()));
        result1.forEach((key, value) -> System.out.println(key + "," + value));
    }
}
