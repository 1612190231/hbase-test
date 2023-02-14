package com.luck.service.impl;

import com.luck.entity.THRegionInfo;
import com.luck.entity.THServerInfo;
import com.luck.service.DealServer;
import com.luck.service.OperateService;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DealServerImpl implements DealServer {
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public Map<String, Object> structureInfos() {
        // 总的 数据规模、region数量、查询命中次数
        long allRegionSize = 0;
        long allRegionCount = 0;
        long allHitCount = 0;
        // 数据获取
        Map<String, THRegionInfo> regionInfoMap = new HashMap<>();
        OperateService regionService = new OperateServiceImpl();
        regionService.init("region_performance", "data");
        ResultScanner results = regionService.getByTable();
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells){
                try {
                    String rowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()); // 获取rowkey
                    String familyName = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()); // 获取列族名
                    String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()); // 获取列名
                    long value = Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//                    logUtil.print("数据的rowkey为" + rowkey + "列族名为" + familyName + "列名为" + columnName + "列的值为" + value);

                    THRegionInfo thRegionInfo = null;
                    if (regionInfoMap.containsKey(rowkey)) thRegionInfo = regionInfoMap.get(rowkey);
                    else {
                        thRegionInfo = new THRegionInfo();
                        regionInfoMap.put(rowkey, thRegionInfo);
                    }
                    thRegionInfo.setRegionName(rowkey);
                    switch (columnName) {
                        case "decayTime":
                            thRegionInfo.setDecayTime(value);
                            break;
                        case "regionHitCount":
                            thRegionInfo.setHitCount(value);
                            break;
                        case "regionSize":
                            thRegionInfo.setRegionSize(value);
                            break;
                        case "startKey":
                            thRegionInfo.setStartKey(String.valueOf(value));
                            break;
                        case "endKey":
                            thRegionInfo.setEndKey(String.valueOf(value));
                            break;
                        default:
                            logger.info(rowkey + "-" + columnName + "-" + value + "报错......");
                    }
                } catch (Exception e){
                    logger.info(e.toString());
                }
            }
        }

        Map<String, THServerInfo> thServerInfoMap = new HashMap<>();
        OperateService serverService = new OperateServiceImpl();
        serverService.init("server_performance", "data");
        ResultScanner serverScanner = serverService.getByTable();
        for (Result result : serverScanner) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells){
                try {
                    String rowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()); // 获取rowkey
                    String familyName = Bytes.toString(cell.getFamilyArray(),cell.getFamilyOffset(),cell.getFamilyLength()); // 获取列族名
                    String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()); // 获取列名
                    long value = Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());
//                    logUtil.print("数据的rowkey为" + rowkey + "列族名为" + familyName + "列名为" + columnName + "列的值为" + value);

                    THServerInfo thServerInfo = null;
                    if (thServerInfoMap.containsKey(rowkey)) thServerInfo = thServerInfoMap.get(rowkey);
                    else {
                        thServerInfo = new THServerInfo();
                        thServerInfoMap.put(rowkey, thServerInfo);
                    }
                    thServerInfo.setServerName(ServerName.valueOf(rowkey));
                    switch (columnName) {
                        case "regionCount":
                            thServerInfo.setRegionCount(value);
                            allRegionCount += value;
                            break;
                        case "sumHitCount":
                            thServerInfo.setSumHitCount(value);
                            allHitCount += value;
                            break;
                        case "sumRegionSize":
                            thServerInfo.setSumRegionSize(value);
                            allRegionSize += value;
                            break;
                        default:
                            logger.info(rowkey + "-" + columnName + "-" + value + "报错......");
                    }
                } catch (Exception e){
                    logger.info(e.toString());
                }
            }
        }

        // 数据构造
        List<THServerInfo> thServerInfos = new ArrayList<>(thServerInfoMap.values());
        for (THServerInfo thServerInfo: thServerInfos){
            for (Map.Entry<String, THRegionInfo> entry: regionInfoMap.entrySet()){
                if (thServerInfo.getServerName().toString().split(",")[0].equals(entry.getKey().split(";")[0])) {
                    if (thServerInfo.getRegionInfos() == null) {
                        List<THRegionInfo> thRegionInfos = new ArrayList<>();
                        thRegionInfos.add(entry.getValue());
                        thServerInfo.setRegionInfos(thRegionInfos);
                    }
                    else {
                        thServerInfo.getRegionInfos().add(entry.getValue());
                    }
                }
            }
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("thServerInfos", thServerInfos);
        resultMap.put("allRegionSize", allRegionSize);
        resultMap.put("allRegionCount", allRegionCount);
        resultMap.put("allHitCount", allHitCount);
        return  resultMap;
    }
}
