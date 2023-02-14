package com.luck.utils;

import com.luck.entity.BaseInfo;
import com.luck.entity.THRegionInfo;
import com.luck.entity.THServerInfo;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ThreadUtil extends Thread{
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    List<String> rowKeys;
    long selectTime;

    public ThreadUtil(List<String> rowKeys, long selectTime) {
        this.rowKeys = rowKeys;
        this.selectTime = selectTime;
    }
    public void run() {
        OperateService operateService = new OperateServiceImpl();
        operateService.init("select_log", "data");
        operateService.createTable("select_log", "data");

        Map<String, String> maps = new HashMap<>();
        try {
            maps = operateService.getByRowKey("rowKey");
        }
        catch (Exception e) {
            logger.info(e.toString());
        }
//
//        String[] rowKeyList = rowKeys.replaceAll(",*$", "").split(",");
//        OperateService regionService = new OperateServiceImpl();
//        regionService.init("region_performance", "data");
////        Filter filter = new QualifierFilter(CompareOperator.GREATER_OR_EQUAL, new SubstringComparator("Key"));
//        ResultScanner resultScanner = regionService.getByTable();
//        Map<String, THRegionInfo> thRegionInfoMap = new HashMap<>();
//        for (Result result: resultScanner) {
//            List<Cell> cells = result.listCells();
//            for (Cell cell : cells){
//                try {
//                    String rowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()); // 获取rowkey
//                    String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()); // 获取列名
//                    String value = Bytes.toString(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//
//                    THRegionInfo thRegionInfo;
//                    if (thRegionInfoMap.containsKey(rowkey)) thRegionInfo = thRegionInfoMap.get(rowkey);
//                    else {
//                        thRegionInfo = new THRegionInfo();
//                        thRegionInfoMap.put(rowkey, thRegionInfo);
//                    }
//                    thRegionInfo.setRegionName(rowkey);
//                    switch (columnName) {
//                        case "startKey":
//                            thRegionInfo.setStartKey(value);
//                            break;
//                        case "endKey":
//                            thRegionInfo.setEndKey(value);
//                            break;
//                        default:
//                            logger.info(rowkey + "-" + columnName + "-" + value + "报错......");
//                    }
//                } catch (Exception e){
//                    logger.info(e.toString());
//                }
//            }
//        }
//        for (THRegionInfo thRegionInfo: thRegionInfoMap.values()){
//
//        }

        if (!maps.isEmpty()) {
            String[] res = maps.get("data").replace("[", "").replace("]", "").split(",");
            this.rowKeys.addAll(Arrays.asList(res));
        }

        // 把数据插入select_log
        BaseInfo baseInfo = new BaseInfo();
        baseInfo.setRowKey("rowKey");
        baseInfo.setColumnFamilyList(new ArrayList<>(Collections.singleton("data")));
        baseInfo.setColumnsList(Collections.singletonList(new HashMap<String, Object>() {{put("data", rowKeys);}}));
        List<BaseInfo> baseInfos = new ArrayList<>();
        baseInfos.add(baseInfo);
        try {
            operateService.addByMutator(baseInfos);
        } catch (Exception e) {
            logger.error(e.toString());
        }
    }
}
