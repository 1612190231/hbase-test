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
                    switch (columnName) {
                        case "decayTime":
                            thRegionInfo.setDecayTime(value);
                            break;
                        case "regionHitCount":
                            thRegionInfo.setHitCount(value);
                            allHitCount += value;
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

        // 找出需要处理的过热region
        double avgHitCount = !regionInfoMap.isEmpty() ? (allHitCount * 1.0 / regionInfoMap.size()) * 1.1 : 0; // 系数1.1
        Map<String, THRegionInfo> hotMap = new TreeMap<>();
        for (THRegionInfo thRegionInfo: regionInfoMap.values()) {
            if (thRegionInfo.getHitCount() > avgHitCount) {
                // 写入较热map, 根据startKey排序
                hotMap.put(thRegionInfo.getStartKey(), thRegionInfo);
            }
        }

        Set<String> serverNames = new HashSet<>();
        String[] serverList = new String[regionInfoMap.size()];
        THRegionInfo[] thRegionInfos = new THRegionInfo[regionInfoMap.size()];
        int length = 0;
        for (THRegionInfo thRegionInfo: hotMap.values()){
            String serverName = thRegionInfo.getRegionName().split(";")[0];
            serverList[length] = serverName;
            serverNames.add(serverName);
            thRegionInfos[length++] = thRegionInfo;
        }

        // 开始重分配region
        double volumeHitCount = allHitCount * 1.3 / 3;            // 节点的hitCount阈值
        Map<String, Double> serverHitCountMap = new HashMap<>();  // 节点当前hitCount
        Map<String, String> serverEndKeyMap = new HashMap<>();    // 节点最大endKey
        String[] targetServers = new String[length];              // region目标节点
        for (int i = 0; i < length; i++) {
            // 尽可能 拒绝相邻region分配在同一server中
            String nearServer = null;
            String targetServer = null;
            for (Map.Entry<String, String> entry: serverEndKeyMap.entrySet()) {
                if (thRegionInfos[i].getStartKey().equals(entry.getValue())) {
                    nearServer = entry.getKey();
                }
            }
            // 在本区 无相邻
            if (!serverList[i].equals(nearServer)) {
                // 没超出hitCount上限
                if (serverHitCountMap.getOrDefault(serverList[i], (double) 0) + thRegionInfos[i].getHitCount() < volumeHitCount)
                    targetServer = serverList[i];
                // 超出上限
                else {
                    for (String serverName: serverNames) {
                        if (serverName.equals(serverList[i])) continue;
                        // serverList[i]移动到非本区 不相邻server
                        if (!serverName.equals(nearServer) &&
                                serverHitCountMap.getOrDefault(serverName, (double) 0) + thRegionInfos[i].getHitCount() < volumeHitCount) {
                            targetServer = serverName;
                        }
                        // serverList[i] 移动到相邻region所在server
                        else {
                            targetServer = nearServer;
                        }
                        break;
                    }
                }
            }
            // 在本区 相邻
            else if (serverList[i].equals(nearServer)) {
                boolean flag = false;
                for (String serverName: serverNames) {
                    if (serverName.equals(nearServer)) continue;
                    // serverList[i]移动到非本区server
                    if (serverHitCountMap.getOrDefault(serverName,  (double) 0) + thRegionInfos[i].getHitCount() < volumeHitCount) {
                        targetServer = serverName;
                        flag = true;
                        break;
                    }
                }
                // 非本区server已满，只能取本区相邻region
                if (!flag) {
                    targetServer = serverList[i];
                }
            }

            targetServers[i] = targetServer;
            serverHitCountMap.put(targetServer, serverHitCountMap.getOrDefault(targetServer, (double) 0) + thRegionInfos[i].getHitCount());
            serverEndKeyMap.put(targetServer, thRegionInfos[i].getEndKey());
        }

        // 开始 move region
        OperateService operateService = new OperateServiceImpl();
        operateService.init("track_mine", "data");
        for (int i = 0; i < length; i++) {
            if (serverList[i].equals(targetServers[i])) {
                logUtil.print("region: " + thRegionInfos[i].getRegionName() + " isn't move...");
                continue;
            }
            operateService.moveRegion(thRegionInfos[i].getRegionName(), ServerName.valueOf(targetServers[i]));
            logUtil.print("region: " + thRegionInfos[i].getRegionName() + "'s old server is " + serverList[i] +
                    ", now move to " + targetServers[i] + "!!!");
        }
    }
}