package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import com.luck.utils.ExcelUtil;
import org.apache.hadoop.hbase.client.ResultScanner;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class HbaseMain {

    public static void main(String[] args) {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //获取数据源
        ExcelUtil excelUtil = new ExcelUtil();
        String path = System.getProperty("user.dir") + "/src/main/resources/test1000.xls";
        File file = new File(path);
        List excelList = excelUtil.readExcel(file);
//        System.out.println(System.getProperty(path));//user.dir指定了当前的路径

        //开始hbase操作
        //初始化
        OperateService operateService = new OperateServiceImpl();
        operateService.setSeries("flow,coordinate,info");
        operateService.setTableName("truckTrace");
        operateService.init();

        //创建表
//        String series = operateService.getSeries();
//        String tableName = operateService.getTableName();
//        operateService.createTable(tableName,series);

        //添加数据集---按rowKey
        long startTime=System.currentTimeMillis(); //获取开始时间
        for (int i = 0; i < excelList.size(); i++) {
            List list = (List) excelList.get(i);
//            logUtil.prepareLog(list);

            BaseInfo baseInfo = new BaseInfo();
            List<String> columnFamilyList = new ArrayList<>();
            List<Map<String, Object>> columnsList = new ArrayList<>();
            String rowKey = list.get(0).toString() + '-' + list.get(3).toString();

            int flag = 0;
            for (int j = 0; j < list.size(); j++) {
                flag = 0;
//                System.out.print(list.get(j));
                try{
                    Map<String, Object> columns1 = new HashMap<String, Object>();
                    columns1.put("district", list.get(1).toString());
                    columns1.put("city", list.get(2).toString());
                    columns1.put("province", list.get(6).toString());
                    columnFamilyList.add("flow");
                    columnsList.add(columns1);
//                operateService.add("flow", rowKey, columns1);

                    Map<String, Object> columns2 = new HashMap<String, Object>();
                    columns2.put("longitude", list.get(5).toString());
                    columns2.put("latitude", list.get(4).toString());
                    columnFamilyList.add("coordinate");
                    columnsList.add(columns2);
//                operateService.add("coordinate", rowKey, columns2);

                    Map<String, Object> columns3 = new HashMap<String, Object>();
                    columns3.put("speed", list.get(7).toString());
                    columnFamilyList.add("info");
                    columnsList.add(columns3);
//                operateService.add("info", rowKey, columns3);
                } catch (Exception e){
                    flag = 1;
                }
            }
            if (flag == 1){
                continue;
            }
            baseInfo.setRowKey(rowKey);
            baseInfo.setColumnFamilyList(columnFamilyList);
            baseInfo.setColumnsList(columnsList);
            operateService.addByRowKey(baseInfo);
        }
        long endTime=System.currentTimeMillis(); //获取结束时间
        logUtil.runTimeLog("addAll", endTime, startTime);

//        //添加数据集---按列族
//        long startTime=System.currentTimeMillis(); //获取开始时间
//        for (int i = 0; i < excelList.size(); i++) {
//            List list = (List) excelList.get(i);
//            logUtil.prepareLog(list);
//
//            String rowKey = list.get(0).toString() + '-' + list.get(3).toString();
//            for (int j = 0; j < list.size(); j++) {
//                System.out.print(list.get(j));
//
//                Map<String, Object> columns1 = new HashMap<String, Object>();
//                columns1.put("district", list.get(1).toString());
//                columns1.put("city", list.get(2).toString());
//                columns1.put("province", list.get(6).toString());
//                operateService.add("flow", rowKey, columns1);
//
//                Map<String, Object> columns2 = new HashMap<String, Object>();
//                columns2.put("longitude", list.get(5).toString());
//                columns2.put("latitude", list.get(4).toString());
//                operateService.add("coordinate", rowKey, columns2);
//
//                Map<String, Object> columns3 = new HashMap<String, Object>();
//                columns3.put("speed", list.get(7).toString());
//                operateService.add("info", rowKey, columns3);
//
//            }
//        }
//        long endTime=System.currentTimeMillis(); //获取结束时间
//        logUtil.runTimeLog("addAll", endTime, startTime);

        //查询数据1-1
//        Map<String, String> map1=  operateService.getAllValue(rowKey1);
//        for (Map.Entry<String, String> entry : map1.entrySet()) {
//            System.out.println("map1-"+entry.getKey()+":"+entry.getValue());
//        }

        //查询数据2
//        String original_data_value =  operateService.getValueBySeries("rowKey","original_data");
//        System.out.println("original_data_value->"+original_data_value);

        //查看表中所有数据
        long startTime2=System.currentTimeMillis(); //获取开始时间
        ResultScanner rs = operateService.getValueByTable();
        long endTime2=System.currentTimeMillis(); //获取结束时间
        logUtil.runTimeLog("getValueByTable", endTime2, startTime2);
//        logUtil.getValueByTable(rs);

        //根据rowKey前缀查询记录
        long startTime3=System.currentTimeMillis(); //获取开始时间
        ResultScanner rsByKey = operateService.getValueByPreKey("黑MN7991");
        long endTime3=System.currentTimeMillis(); //获取结束时间
        logUtil.runTimeLog("getValueByPreKey", endTime3, startTime3);
        logUtil.getValueByTable(rsByKey);
    }

}
