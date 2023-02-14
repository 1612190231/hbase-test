package com.luck.monitor;

import java.io.IOException;
import java.util.*;

import com.luck.entity.BaseInfo;
import com.luck.entity.THRegionInfo;
import com.luck.entity.THServerInfo;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WALEdit;

public class ObserverMonitor {
    public static void main(String[] args) throws IOException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        OperateService operateService = new OperateServiceImpl();
        operateService.init("track_mine", "data");
        OperateService regionService = new OperateServiceImpl();
        regionService.init("region_performance", "data");
        regionService.createTable("region_performance", "data");
        OperateService serverService = new OperateServiceImpl();
        serverService.init("server_performance", "data");
        serverService.createTable("server_performance", "data");

        // 用来计算真实的查询命中次数
        OperateService selectService = new OperateServiceImpl();
        selectService.init("select_log", "data");
        Map<String, String> maps;
        try {
            maps = selectService.getByRowKey("rowKey");
//            logUtil.print(maps.get("data"));
        } catch (Exception e) {
            logUtil.print("Table select_log is empty!!!!!");
            return;
        }
        String[] res = maps.get("data").replaceAll(" ", "").replace("[", "").replace("]", "").split(",");
//        for (String rr: res){
//            logUtil.print(rr);
//        }

        // 获取regions价值数据
        List<THServerInfo> thServerInfos = operateService.getRegionsStatus(res);

        // 清除原始表数据
        regionService.truncateTable();
        serverService.truncateTable();

        // 把各数据插入维护的价值表中
        List<BaseInfo> baseRegionInfos = new ArrayList<>();
        List<BaseInfo> baseServerInfos = new ArrayList<>();
        for (THServerInfo thServerInfo: thServerInfos){
            // 插入regionServer性能表
            BaseInfo baseServerInfo = new BaseInfo();
            baseServerInfo.setRowKey(thServerInfo.getServerName().toString());
            baseServerInfo.setColumnFamilyList(new ArrayList<>(Collections.singleton("data")));

            List<Map<String, Object>> serverColumnsList = new ArrayList<>();
            Map<String, Object> serverColumn = new HashMap<String, Object>(){{
                put("regionCount", thServerInfo.getRegionCount());
                put("sumHitCount", thServerInfo.getSumHitCount());
                put("sumRegionSize", thServerInfo.getSumRegionSize());
            }};
            serverColumnsList.add(serverColumn);
            baseServerInfo.setColumnsList(serverColumnsList);
            baseServerInfos.add(baseServerInfo);

            // 插入region性能表
            for (THRegionInfo thRegionInfo: thServerInfo.getRegionInfos()){
                BaseInfo baseRegionInfo = new BaseInfo();
                baseRegionInfo.setRowKey(thServerInfo.getServerName().toString().split(",")[0]
                        + ";" + thRegionInfo.getRegionName());
                baseRegionInfo.setColumnFamilyList(new ArrayList<>(Collections.singleton("data")));

                // 插入regionSize、regionHitCount、decayTime
                List<Map<String, Object>> regionColumnsList = new ArrayList<>();
                Map<String, Object> regionColumn = new HashMap<String, Object>(){{
                    put("startKey", Long.parseLong(thRegionInfo.getStartKey()));
                    put("endKey", Long.parseLong(thRegionInfo.getEndKey()));
                    put("regionSize", thRegionInfo.getRegionSize());
                    put("regionHitCount", thRegionInfo.getHitCount());
                    put("decayTime", (Long.parseLong(thRegionInfo.getEndKey().substring(2, 5))
                            + Long.parseLong(thRegionInfo.getStartKey().substring(2, 5))) / 2);
                }};
                regionColumnsList.add(regionColumn);
                baseRegionInfo.setColumnsList(regionColumnsList);

                baseRegionInfos.add(baseRegionInfo);
            }
        }
        regionService.addByListPut(baseRegionInfos);
        serverService.addByListPut(baseServerInfos);

        logUtil.print("monitor SUCCESS!!!!!!!");

    }
}