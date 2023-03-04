package com.luck.monitor;

import com.luck.entity.THRegionInfo;
import com.luck.entity.THServerInfo;
import com.luck.service.DealServer;
import com.luck.service.OperateService;
import com.luck.service.impl.DealServerImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

public class StrategicServerMaker {
    public static void main(String[] args) throws IOException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        // 总的 数据规模、region数量、查询命中次数
        // 数据获取
        long allHitCount = 0;
        Map<String, Double> serverHitCountMap = new HashMap<>();  // 节点当前hitCount
        Map<String, THRegionInfo> regionInfoMap = new HashMap<>();
        OperateService regionService = new OperateServiceImpl();
        regionService.init("region_performance", "data");
        ResultScanner results = regionService.getByTable();
        for (Result result : results) {
            List<Cell> cells = result.listCells();
            for (Cell cell : cells){
                try {
                    String rowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()); // 获取rowkey
                    String columnName = Bytes.toString(cell.getQualifierArray(),cell.getQualifierOffset(),cell.getQualifierLength()); // 获取列名
                    long value = Bytes.toLong(cell.getValueArray(),cell.getValueOffset(),cell.getValueLength());

                    THRegionInfo thRegionInfo = null;
                    if (regionInfoMap.containsKey(rowkey)) thRegionInfo = regionInfoMap.get(rowkey);
                    else {
                        thRegionInfo = new THRegionInfo();
                        regionInfoMap.put(rowkey, thRegionInfo);
                    }
                    thRegionInfo.setRegionName(rowkey);
                    ServerName serverName = regionService.getServerName(thRegionInfo.getRegionName().split(";")[0]);
                    if (!serverHitCountMap.containsKey(serverName.getServerName())) {
                        serverHitCountMap.put(serverName.getServerName(), 0.0);
                    }
                    switch (columnName) {
                        case "decayTime":
                            thRegionInfo.setDecayTime(value);
                            break;
                        case "regionHitCount":
                            thRegionInfo.setHitCount(value);
                            allHitCount += value;
                            serverHitCountMap.put(serverName.getServerName(), serverHitCountMap.getOrDefault(serverName.getServerName(), 0.0) + value);
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
                            logUtil.print(rowkey + "-" + columnName + "-" + value + "报错......");
                    }
                } catch (Exception e){
                    logUtil.print(e.toString());
                }
            }
        }

        double avgHitCount = !regionInfoMap.isEmpty() ? (allHitCount * 1.0 / regionInfoMap.size()) * 1.1 : 0; // 系数1.1
        Map<String, THRegionInfo> hotMap = new TreeMap<>();
        for (THRegionInfo thRegionInfo: regionInfoMap.values()) {
            if (thRegionInfo.getHitCount() > avgHitCount) {
                // 写入较热map, 根据startKey排序
                hotMap.put(thRegionInfo.getRegionName(), thRegionInfo);
            }
        }
        // 找出需要处理的过热region
        // 找出查询命中低于阈值的server，从小到大排序
        List<Map.Entry<String,THRegionInfo>> hotList = new ArrayList<>(hotMap.entrySet());
        // value.hitCount降序排序
        hotList.sort((o1, o2) -> Long.compare(o2.getValue().getHitCount(), o1.getValue().getHitCount()));
//        hotList.sort(Comparator.comparingLong(o -> o.getValue().getHitCount()));
        // value升序排序
        List<Map.Entry<String,Double>> serverList = new ArrayList<>(serverHitCountMap.entrySet());
        serverList.sort(Map.Entry.comparingByValue());

        logUtil.print("hotList: "+ hotList);
        logUtil.print("serverList: "+ serverList);

        // 开始重分配region
        double volumeHitCount = allHitCount * 1.3 / 3;            // 节点的hitCount阈值
        List<String> sourceRegions = new ArrayList<>();
        List<String> targetServers = new ArrayList<>();              // region目标节点
        for (Map.Entry<String, THRegionInfo> hotEntry : hotList) {
            for (Map.Entry<String, Double> serverHitCountEntry : serverList) {
                if (hotEntry.getKey().split(";")[0].equals(serverHitCountEntry.getKey())) continue;
                if (hotEntry.getValue().getHitCount() + serverHitCountEntry.getValue() < volumeHitCount) {
                    sourceRegions.add(hotEntry.getValue().getRegionName());
                    targetServers.add(serverHitCountEntry.getKey());
                    serverHitCountMap.put(serverHitCountEntry.getKey(), serverHitCountEntry.getValue() + hotEntry.getValue().getHitCount());
                }
            }
        }
        logUtil.print("sourceRegions: "+ sourceRegions);
        logUtil.print("targetServers: "+ targetServers);

        // 开始 move region
        OperateService operateService = new OperateServiceImpl();
        operateService.init("track_mine", "data");
        for (int i = 0; i < sourceRegions.size(); i++) {
            operateService.moveRegion(sourceRegions.get(i).split("\\.")[1], ServerName.valueOf(targetServers.get(i)));
            logUtil.print("region: " + sourceRegions.get(i) + "'s old server is " + serverList.get(i) + ", now move to " + targetServers.get(i) + "!!!");
        }
    }
}