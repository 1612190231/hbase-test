package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.entity.PointInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.OperateService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.CsvUtil;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.File;
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
public class HbaseShard {

    public static void main(String[] args) throws MalformedURLException, ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
//        File file = new File("file:////home/cklu/data/shard/test_s.csv");
        File file = new File("src/main/resources/test_s.csv");
        URL url = new URL("file:////C:/Users/user/Desktop/code/hbase-test/src/main/resources/test_s.csv");
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

        //开始hbase操作
        //初始化
        OperateService operateService = new OperateServiceImpl();
        operateService.setSeries("data");
        operateService.setTableName("hbase_shard");
        operateService.init();

        //创建表
        String series = operateService.getSeries();
        String tableName = operateService.getTableName();
        operateService.createTable(tableName,series);

        //添加数据集---按rowKey
//        long startTime=System.currentTimeMillis(); //获取开始时间
//        for (Map<String, Object> cell : trajectoryInfos) {
//            //            logUtil.prepareLog(list);
//
//            BaseInfo baseInfo = new BaseInfo();
//            List<String> columnFamilyList = new ArrayList<>();
//            List<Map<String, Object>> columnsList = new ArrayList<>();
//            String rowKey = "111";
//
////          System.out.print(list.get(j));
//            try {
//                columnFamilyList.add("data");
//                columnsList.add(cell);
//            } catch (Exception e) {
////                hbaseShardInsert.error("插入报错rowKey:" + rowKey);
//            }
//
//            baseInfo.setRowKey(rowKey);
//            baseInfo.setColumnFamilyList(columnFamilyList);
//            baseInfo.setColumnsList(columnsList);
//            operateService.addByRowKey(baseInfo);
//        }
//        long endTime=System.currentTimeMillis(); //获取结束时间
//        logUtil.runTimeLog("addAll", endTime, startTime);
//
//        //查看表中所有数据
//        long startTime2=System.currentTimeMillis(); //获取开始时间
//        ResultScanner rs = operateService.getValueByTable();
//        long endTime2=System.currentTimeMillis(); //获取结束时间
//        logUtil.runTimeLog("getValueByTable", endTime2, startTime2);
////        logUtil.getValueByTable(rs);
//
//        //根据rowKey前缀查询记录
//        long startTime3=System.currentTimeMillis(); //获取开始时间
//        ResultScanner rsByKey = operateService.getValueByPreKey("黑MN7991");
//        long endTime3=System.currentTimeMillis(); //获取结束时间
//        logUtil.runTimeLog("getValueByPreKey", endTime3, startTime3);
//        logUtil.getValueByTable(rsByKey);
    }

}
