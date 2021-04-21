package com.luck;

import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.ReadExcel;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class HbaseMain {
    private static String TABLENAME = "truckTrace";

    public static void main(String[] args) throws Exception {
        // 获取数据源
        ReadExcel obj = new ReadExcel();
        String path = System.getProperty("user.dir") + "/src/main/resources/test.xls";
        File file = new File(path);
        System.out.println(System.getProperty(path));//user.dir指定了当前的路径
        List excelList = obj.readExcel(file);

        OperateService operateService = new OperateServiceImpl();

        // 初始化
        operateService.init();

        // 创建表
        operateService.createTable(TABLENAME,"");
        // 添加数据集
        for (int i = 0; i < excelList.size(); i++) {
            List list = (List) excelList.get(i);
            String rowKey = list.get(0).toString() + '-' + list.get(3).toString();
            for (int j = 0; j < list.size(); j++) {
                System.out.print(list.get(j));
                Map<String, Object> columns = new HashMap<String, Object>();
                columns.put("district", list.get(1).toString());
                columns.put("city", list.get(2).toString());
                columns.put("province", list.get(6).toString());
                operateService.add("flow", rowKey, columns);

                columns.put("longitude", list.get(5).toString());
                columns.put("latitude", list.get(4).toString());
                operateService.add("coordinate", rowKey, columns);

                columns.put("speed", list.get(7).toString());
                operateService.add("info", rowKey, columns);

            }
            System.out.println();
        }

        //查询数据1-1
//        Map<String, String> map1=  operateService.getAllValue(rowKey1);
//        for (Map.Entry<String, String> entry : map1.entrySet()) {
//            System.out.println("map1-"+entry.getKey()+":"+entry.getValue());
//        }

        //查询数据2
//        String original_data_value =  operateService.getValueBySeries("rowKey","original_data");
//        System.out.println("original_data_value->"+original_data_value);

        //查看表中所有数据
        operateService.getValueByTable();
    }

}
