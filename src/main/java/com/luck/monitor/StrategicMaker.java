package com.luck.monitor;

import com.luck.entity.THRegionInfo;
import com.luck.entity.THServerInfo;
import com.luck.service.DealServer;
import com.luck.service.OperateService;
import com.luck.service.impl.DealServerImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.ServerName;

import java.io.IOException;
import java.util.*;

public class StrategicMaker {
    public static void main(String[] args) throws IOException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        final long minDecayTime = 505;
        final long maxDecayTime = 846;
//        int MaxRegionSize = 512;
        double alpha = 1.0; // regionSize ≈ alpha * serverRegionSize
        double beta = 1.0;  // (decayTime - minDecayTime) ≈ beta * (maxDecayTime - minDecayTime)
        double gama = 1.0; // hitCount ≈ gama * serverHitCount
        double omega = alpha + 0.3 * beta + 0.7 * gama; // 取平均值

        DealServer dealServer = new DealServerImpl();
        Map<String, Object> resMap = dealServer.structureInfos();
        List<THServerInfo> thServerInfos = (List<THServerInfo>) resMap.get("thServerInfos");
        long allRegionCount = (long) resMap.get("allRegionCount");
        long allRegionSize = (long) resMap.get("allRegionSize");
        long allHitCount = (long) resMap.get("allHitCount");

        // 开始正式逻辑
        // 确定server的状态
        logUtil.print("START TRUE LOGIC COMPUTE!!!!!!!!!!!!!!!!");
        OperateService operateService = new OperateServiceImpl();
        operateService.init("track_mine", "data");
//        for (THServerInfo thServerInfo: thServerInfos) {
////            logUtil.print("-------------------------------------------------------");
//            double regionCountValue = thServerInfo.getRegionCount() * 1.0 / allRegionCount;
//            double regionSizeValue = thServerInfo.getSumRegionSize() * 1.0 / allRegionSize;
//            double hitCountValue = thServerInfo.getSumHitCount() * 1.0 / allHitCount;
//
//            double maxFinalValue = Math.max(regionCountValue, Math.max(regionSizeValue, hitCountValue));
//            double minFinalValue = Math.min(regionCountValue, Math.min(regionSizeValue, hitCountValue));
//
//            if (maxFinalValue > 0.4) { thServerInfo.setServerStatus(1); }
//            else if (minFinalValue < 0.25) { thServerInfo.setServerStatus(-1); }
////            logUtil.print(thServerInfo.getServerName() + "'s serverStatus is " + thServerInfo.getServerStatus());
//        }


        SortedMap<String, THRegionInfo> sortedMap = new TreeMap();
        for (THServerInfo thServerInfo: thServerInfos) {

            logUtil.print("--------------------Server Compute-----------------------");
            /* 总原则：
            * 四个条件：分区数量、分区大小、查询命中次数、时间衰减因子
            * 一个正常，两个不正常，可能没问题，要看两个加起来的比例
            * 两个正常，一个不正常，肯定有问题
            * */
            long serverRegionCount = thServerInfo.getRegionCount();

            long serverRegionSize = thServerInfo.getSumRegionSize();
            double avgRegionSize = serverRegionCount == 0 ? 0 : serverRegionSize * 1.0 / serverRegionCount;  // 平均分区数据密度
            long serverHitCount = thServerInfo.getSumHitCount();
            logUtil.print("serverRegionCount: " + serverRegionCount);

            logUtil.print("serverRegionSize: " + serverRegionSize);
            logUtil.print("avgRegionSize: " + avgRegionSize);
            logUtil.print("serverHitCount: " + serverHitCount);

//            if (thServerInfo.getServerStatus() == 0) continue;

            boolean flag = false; // 判定该server内的regions是否有异常，有异常则true，无异常则默认
            for (THRegionInfo thRegionInfo: thServerInfo.getRegionInfos()) {
                logUtil.print("---------------------Region Compute----------------------");
                logUtil.print("omega: " + omega * serverRegionCount / allRegionCount);
                // regionSize 越大、hitCount 越小
                // regionSize 越大、decayTIme 越小

                long regionSize = thRegionInfo.getRegionSize();
                long hitCount = thRegionInfo.getHitCount();
                long decayTime = thRegionInfo.getDecayTime();
                double avgHitCount = serverRegionCount == 0 ? 0 : serverHitCount * 1.0 / serverRegionCount;
                logUtil.print("regionSize: " + regionSize);
                logUtil.print("hitCount: " + hitCount);
                logUtil.print("decayTime: " + decayTime);
                logUtil.print("avgHitCount: " + avgHitCount);

                double alphaT = (serverRegionSize == 0) ? 0 : regionSize * 1.0 / (serverRegionSize * 1.0);
                double betaT = (decayTime - minDecayTime) * 1.0 / (maxDecayTime - minDecayTime);
                double gamaT = (serverHitCount == 0) ? 0 : hitCount * 1.0 / (serverHitCount * 1.0);
                double omegaT = alphaT + 0.3 * betaT + 0.7 * gamaT;
                logUtil.print("alphaT: " + alphaT);
                logUtil.print("betaT: " + betaT);
                logUtil.print("gamaT: " + gamaT);
                logUtil.print("omegaT: " + omegaT);

                if (omegaT > 1.05 * omega * serverRegionCount / allRegionCount) {
//                if (omegaT > 1.05 * omega * serverRegionCount / allRegionCount || thRegionInfo.getRegionSize() > MaxRegionSize) {
                    // need split
                    logUtil.print(thRegionInfo.getRegionName() + "START SPLIT!!!!!!");

                    thRegionInfo.setRegionStatus(1);
                    flag = true;
                    try {
                        boolean result = operateService.splitRegion(thRegionInfo.getRegionName().split(";")[1]);
                        thRegionInfo.setHasDeal(true);
                    } catch (Exception e) {
                        logUtil.print(e.toString());
                    }
                    logUtil.print(thRegionInfo.getRegionName() + " SPLIT SUCCESS!!!!!!");
                }
                else if (omegaT < 0.95 * omega * serverRegionCount / allRegionCount) {
                    // need merge
                    logUtil.print(thRegionInfo.getRegionName() + " need merge......");
                    thRegionInfo.setRegionStatus(-1);
                    sortedMap.put(thRegionInfo.getStartKey(), thRegionInfo);
                    flag = true;
                }
            }
            if (!flag) {
                // need move
                thServerInfo.setNeedMove(true);
                logUtil.print(thServerInfo.getServerName() + " needs move!!!!!!");
            }
        }
        while (sortedMap.size() >1) {
            logUtil.print("=====================merge start====================");
            try {
                THRegionInfo thRegionInfo1 = sortedMap.get(sortedMap.firstKey());
                logUtil.print("first key is: " + sortedMap.firstKey());
                sortedMap.remove(sortedMap.firstKey());
                thRegionInfo1.setHasDeal(true);
                THRegionInfo thRegionInfo2 = sortedMap.get(sortedMap.firstKey());
                logUtil.print("second key is: " + sortedMap.firstKey());
//                if (thRegionInfo1.getRegionSize() + thRegionInfo2.getRegionSize() > MaxRegionSize) {
//                    logUtil.print("Regions' size are larger than max region size, can not be merged!!!!!");
//                    continue;
//                }
                if (! thRegionInfo1.getEndKey().equals(thRegionInfo2.getStartKey())) {
                    logUtil.print("Unable to merge non-adjacent or non-overlapping regions!!!!!");
                    continue;
                }
                sortedMap.remove(sortedMap.firstKey());
                thRegionInfo2.setHasDeal(true);

                // start merge
                logUtil.print(thRegionInfo1.getRegionName().split(";")[1] + " AND " +
                        thRegionInfo2.getRegionName().split(";")[1] + " START MERGE!!!!!!");
                operateService.mergeRegion(thRegionInfo1.getRegionName().split(";")[1],
                        thRegionInfo2.getRegionName().split(";")[1]);
            } catch (Exception e) {
                logUtil.print(e.toString());
                logUtil.print("=====================merge error====================");
            }
        }

        // 开始处理需要move的servers
        List<THRegionInfo> targetHotRegions = new ArrayList<>();
        List<ServerName> targetColdServers = new ArrayList<>();
        for (THServerInfo thServerInfo: thServerInfos) {
            if (thServerInfo.getServerStatus() == 1 && thServerInfo.isNeedMove()) {
                targetHotRegions.addAll(thServerInfo.getRegionInfos());
            }
            else if (thServerInfo.getServerStatus() == -1 && thServerInfo.isNeedMove()) {
                targetColdServers.add(thServerInfo.getServerName());
            }
        }
        Iterator<ServerName> coldIterator = targetColdServers.iterator();
        Iterator<THRegionInfo> hotIterator = targetHotRegions.iterator();
        while (coldIterator.hasNext() && hotIterator.hasNext()){
            logUtil.print(hotIterator.next().getRegionName() + "is moving!!!");
            operateService.moveRegion(hotIterator.next().getRegionName().split(";")[1], coldIterator.next());
        }
    }
}