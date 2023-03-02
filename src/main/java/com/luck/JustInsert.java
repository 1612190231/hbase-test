package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.OperateService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.*;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class JustInsert {

    public static void main(String[] args) throws MalformedURLException, ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();
        long startTime=System.currentTimeMillis(); //获取开始时间

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
//        URL url = new URL("file:////C:\\Users\\user\\Desktop\\code\\hbase-test\\src\\main\\resources\\-1010.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
        URL url = new URL(args[0]);
        List<TrajectoryInfo> trajectoryInfos = hbaseShardService.getTrajectoryInfos(url);

        // 构造索引
        trajectoryInfos = hbaseShardService.dimensionReduction(trajectoryInfos);

        // 添加数据集---按rowKey
        List<BaseInfo> baseInfos = new ArrayList<>();
        List<String> shardRowKeys = new ArrayList<>();
        for (TrajectoryInfo trajectoryInfo : trajectoryInfos) {
            if (trajectoryInfo.getKeyRange() == null || trajectoryInfo.getKeyTime() == null){
                continue;
            }
            BaseInfo baseInfo = new BaseInfo();
            List<String> columnFamilyList = new ArrayList<>();
            List<Map<String, Object>> columnsList = new ArrayList<>();

            int ran = (int) (Math.random() * 100);
            String numHash = String.format("%03d", ran);
            String keySum = trajectoryInfo.getKeyTime() + trajectoryInfo.getKeyRange();
            String rowKey = numHash + keySum + trajectoryInfo.getVehicleNo();
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
        operateService.setTableName("track_just");
        operateService.init();

        // 创建表
        String series = operateService.getSeries();
        String tableName = operateService.getTableName();
        operateService.createTable(tableName,series);

        // 插入数据
        operateService.addByMutator(baseInfos);
        long endTime=System.currentTimeMillis(); //获取结束时间
        logUtil.runTimeLog("addAll", endTime, startTime);
    }
}
