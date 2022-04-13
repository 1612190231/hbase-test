package com.luck.service.impl;

import com.luck.entity.PointInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.index.XZIndexing;
import com.luck.service.HbaseShardService;
import com.luck.utils.ByteUtil;
import com.luck.utils.CsvUtil;
import org.apache.commons.csv.CSVRecord;

import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author luchengkai
 * @description 分区实现类
 * @date 2021/11/26 1:37
 */
public class HbaseShardServiceImpl implements HbaseShardService {
    public List<TrajectoryInfo> getTrajectoryInfos(URL url) throws ParseException {
        CsvUtil csvUtil = new CsvUtil();
        List<CSVRecord> list = csvUtil.readCsv(url);
        Map<String, TrajectoryInfo> trajectoryInfoMap = new HashMap<>();
        List<TrajectoryInfo> trajectoryInfos = new ArrayList<>();
        DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        long init_date = df.parse("01/01/2020 00:00:00").getTime();
        for ( CSVRecord items: list){
            if(items.get(0).compareTo("truck_no") == 0){ continue; }
            long target_date = df.parse(items.get(1)).getTime();
            long diff = target_date - init_date;
            long days = diff / (1000 * 60 * 60 * 24);
            String numString = String.format("%05d", days);
            double lat = Float.parseFloat(items.get(2));
            double lon = Float.parseFloat(items.get(3));
            String judgeString = items.get(0) + numString;
            if (!trajectoryInfoMap.containsKey(judgeString)){
                TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
                List<PointInfo> pointInfos = new ArrayList<>();
                PointInfo pointInfo = new PointInfo(items.get(0), items.get(1), lat, lon);
                pointInfos.add(pointInfo);
                trajectoryInfo.setVehicleNo(items.get(0));
                trajectoryInfo.setPointInfos(pointInfos);
                // 时间戳
                trajectoryInfo.setKeyTime(numString);
                trajectoryInfo.setMaxTime(target_date);
                trajectoryInfo.setMinTime(target_date);

                // mbr
                trajectoryInfo.setMaxLat(lat);
                trajectoryInfo.setMinLat(lat);
                trajectoryInfo.setMaxLon(lon);
                trajectoryInfo.setMinLon(lon);

                trajectoryInfoMap.put(judgeString, trajectoryInfo);
            }
            else {
                TrajectoryInfo trajectoryInfo = trajectoryInfoMap.get(judgeString);
                PointInfo pointInfo = new PointInfo(items.get(0), items.get(1), lat, lon);
                trajectoryInfo.getPointInfos().add(pointInfo);
                // 时间戳
                if (target_date > trajectoryInfo.getMaxTime()){
                    trajectoryInfo.setMaxTime(target_date);
                }
                if (target_date < trajectoryInfo.getMinTime()){
                    trajectoryInfo.setMinTime(target_date);
                }
                // mbr
                if (lat < trajectoryInfo.getMinLat()){
                    trajectoryInfo.setMinLat(lat);
                }
                if (lat > trajectoryInfo.getMaxLat()){
                    trajectoryInfo.setMaxLat(lat);
                }
                if (lon < trajectoryInfo.getMinLon()){
                    trajectoryInfo.setMinLon(lon);
                }
                if (lon > trajectoryInfo.getMinLon()){
                    trajectoryInfo.setMaxLon(lon);
                }
            }
        }
        trajectoryInfoMap.forEach((key, value) -> {
            trajectoryInfos.add(value);
        });
        return trajectoryInfos;
    }

    public List<TrajectoryInfo> constructMbr(List<TrajectoryInfo> trajectoryInfos) throws ParseException {
        for(TrajectoryInfo trajectoryInfo: trajectoryInfos){
            double minLat = 999;
            double minLon = 999;
            double maxLat = -999;
            double maxLon = -999;
            for(PointInfo pointInfo: trajectoryInfo.getPointInfos()){
                if (pointInfo.getLat() < minLat){
                    minLat = pointInfo.getLat();
                }
                if (pointInfo.getLat() > maxLat){
                    maxLat = pointInfo.getLat();
                }
                if (pointInfo.getLon() < minLon){
                    minLon = pointInfo.getLon();
                }
                if (pointInfo.getLon() > maxLon){
                    maxLon = pointInfo.getLon();
                }
            }
            trajectoryInfo.setMinLon(minLon);
            trajectoryInfo.setMaxLon(maxLon);
            trajectoryInfo.setMinLat(minLat);
            trajectoryInfo.setMaxLat(maxLat);
            trajectoryInfo.setMidLon((minLon + maxLon) / 2.0);
            trajectoryInfo.setMidLat((minLat + maxLat) / 2.0);
//            trajectoryInfo.setMidPoint = [trajectoryInfo.mid_lon, trajectoryInfo.mid_lat];

            // 构建索引-temporal
//            trajectoryInfo.setKeyTime();
        }
        return trajectoryInfos;
    }

    public List<TrajectoryInfo> dimensionReduction(List<TrajectoryInfo> trajectoryInfos){
        for (TrajectoryInfo trajectoryInfo: trajectoryInfos){
            // 生成范围key
            XZIndexing xzIndexing = new XZIndexing();
            long keyRange = xzIndexing.index((short) 9, trajectoryInfo.getMinLon(),
                    trajectoryInfo.getMinLat(), trajectoryInfo.getMaxLon(), trajectoryInfo.getMaxLat());
            String numString = String.format("%09d", keyRange);
            trajectoryInfo.setKeyRange(numString);
            ByteUtil byteUtil = new ByteUtil();

            // 生成rowKey
            byte[] timeKey = trajectoryInfo.getKeyTime().getBytes(StandardCharsets.UTF_8);
            byte[] rangeKey = trajectoryInfo.getKeyRange().getBytes(StandardCharsets.UTF_8);
            trajectoryInfo.setRowKey(byteUtil.mergeBytes(timeKey, rangeKey));
        }
        return trajectoryInfos;
    }

    private int[] reduction_(Float loc, double bound_min, double bound_max){
        // lat: 32 - 38
        // lon: 115 - 121
        int[] mp_ = new int[3];
        // 判断是否在0的右边
        double mid = (bound_min + bound_max) / 2.0;
        double mid_r = (mid + bound_max) / 2.0;
        double mid_r_r = (mid_r + bound_max) / 2.0;
        double mid_r_l = (mid + mid_r) / 2.0;
        double mid_l = (bound_min + mid) / 2.0;
        double mid_l_r = (mid_l + mid) / 2.0;
        double mid_l_l = (bound_min + mid_l) / 2.0;

        // 判断是否在 mid 右边
        if (loc > mid){
            mp_[0] = 1;
        }

        // 判断是否在 mid_r 右边
        if (loc > mid_r){
            mp_[1] = 1;
        }

        // 判断是否在 mid_r_r 右边
        if (loc > mid_r_r){
            mp_[2] = 1;
        }
        // 判断是否在 mid_r_l 右边
        else if (loc > mid_r_l){
            mp_[2] = 1;
        }
        // 判断是否在 mid_l 右边
        else if (loc > mid_l){
            mp_[1] = 1;
        }

        // 判断是否在 mid_l_r 右边
        if (loc > mid_l_r){
            mp_[2] = 1;
        }
        // 判断是否在 mid_l_l 右边
        else if (loc > mid_l_l){
            mp_[2] = 1;
        }

        return mp_;
    }
}
