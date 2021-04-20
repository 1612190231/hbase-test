package com.luck;

import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class HbaseMain {
    private static String SERIES = "s";
    private static String TABLENAME = "AF_TABLE";

    public static void main(String[] args) throws Exception {
        OperateService operateService = new OperateServiceImpl();

        operateService.init();
        //创建表
        operateService.createTable(TABLENAME,"");
        //添加数据1
        String rowKey1 = "u1001c1001";
        Map<String, Object> columns = new HashMap<String, Object>();
        columns.put("original_data", "original_data_u1001c1001_1");
        columns.put("original_data", "original_data_u1001c1001_2");
        operateService.add(rowKey1,columns);

        //添加数据2
        String rowKey2 = "u1001c1001";
        Map<String, Object> columns2 = new HashMap<String, Object>();
        columns2.put("original_data", "original_data_u1001c1002_1");
        columns2.put("original_data", "original_data_u1001c1002_2");
        operateService.add(rowKey2,columns2);

        //查询数据1-1
        Map<String, String> map1=  operateService.getAllValue(rowKey1);
        for (Map.Entry<String, String> entry : map1.entrySet()) {
            System.out.println("map1-"+entry.getKey()+":"+entry.getValue());
        }

        //查询数据1-2
        Map<String, String> map2=  operateService.getAllValue(rowKey2);
        for (Map.Entry<String, String> entry : map2.entrySet()) {
            System.out.println("map2-"+entry.getKey()+":"+entry.getValue());
        }

        //查询数据2
        String original_data_value =  operateService.getValueBySeries(rowKey1,"original_data");
        System.out.println("original_data_value->"+original_data_value);

        //查看表中所有数据
        operateService.getValueByTable();
    }

}
