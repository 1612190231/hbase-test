package com.luck.service.impl;

import com.luck.entity.PointInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.utils.CsvUtil;
import com.luck.utils.ExcelUtil;
import org.apache.commons.csv.CSVRecord;

import java.io.File;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author luchengkai
 * @description 分区实现类
 * @date 2021/11/26 1:37
 */
public class HbaseShardServiceImpl implements HbaseShardService {
    public List<TrajectoryInfo> getTrajectoryInfos(File file){
        ExcelUtil excelUtil = new ExcelUtil();
        List<List<Object>> list = excelUtil.readExcel(file);
        Map<String, TrajectoryInfo> trajectoryInfoMap = new HashMap<>();
        List<TrajectoryInfo> trajectoryInfos = new ArrayList<>();
        for (List<Object> items: list){
            if (trajectoryInfoMap.containsKey(items.get(0))){
                TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
                List<PointInfo> pointInfos = new ArrayList<>();
                PointInfo pointInfo = new PointInfo(
                        (String)items.get(0),
                        (String)items.get(1),
                        (String)items.get(4),
                        Float.parseFloat((String) items.get(5)),
                        Float.parseFloat((String) items.get(6)));
                pointInfos.add(pointInfo);
                trajectoryInfo.setPlanNo((String)items.get(0));
                trajectoryInfo.setVehicleNo((String)items.get(1));
                trajectoryInfo.setPointInfos(pointInfos);
                trajectoryInfoMap.put((String)items.get(0), trajectoryInfo);
            }
            else {
                TrajectoryInfo trajectoryInfo = trajectoryInfoMap.get(items.get(0));
                PointInfo pointInfo = new PointInfo(
                        (String)items.get(0),
                        (String)items.get(1),
                        (String)items.get(4),
                        Float.parseFloat((String) items.get(5)),
                        Float.parseFloat((String) items.get(6)));
                trajectoryInfo.getPointInfos().add(pointInfo);
            }
        }
        trajectoryInfoMap.forEach((key, value) -> {
            trajectoryInfos.add(value);
        });
        return trajectoryInfos;
    }

    public List<TrajectoryInfo> constructMbr(List<TrajectoryInfo> trajectoryInfos){
        for(TrajectoryInfo trajectoryInfo: trajectoryInfos){
            String minTime = "2970-01-01 00:00:00";
            String maxTime = "1970-01-01 00:00:00";
            float minLat = 999;
            float minLon = 999;
            float maxLat = -999;
            float maxLon = -999;
            for(PointInfo pointInfo: trajectoryInfo.getPointInfos()){
//                if (pointInfo.gtm > maxTime){
//                    maxTime = pointInfo.gtm;
//                }
//                if (pointInfo.gtm < minTime){
//                    minTime = pointInfo.gtm;
//                }
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
//            trajectoryInfo.setMinTime() = minTime;
//            trajectoryInfo.setMaxTime() = maxTime;
            trajectoryInfo.setMinLon(minLon);
            trajectoryInfo.setMaxLon(maxLon);
            trajectoryInfo.setMinLat(minLat);
            trajectoryInfo.setMaxLat(maxLat);
            trajectoryInfo.setMidLon((minLon + maxLon) / 2.0);
            trajectoryInfo.setMidLat((minLat + maxLat) / 2.0);
//            trajectoryInfo.setMidPoint = [trajectoryInfo.mid_lon, trajectoryInfo.mid_lat];
        }
        return trajectoryInfos;
    }

    public List<TrajectoryInfo> dimensionReduction(List<TrajectoryInfo> trajectoryInfos){
        for (TrajectoryInfo trajectoryInfo: trajectoryInfos){
            int[] mpMinLat = reduction_(trajectoryInfo.getMinLat(), 33.5, 37.5);
            int[] mpMinLon = reduction_(trajectoryInfo.getMinLon(), 116.5, 120.5);
            int[] mpMaxLat = reduction_(trajectoryInfo.getMaxLat(), 33.5, 37.5);
            int[] mpMaxLon = reduction_(trajectoryInfo.getMaxLon(), 116.5, 120.5);
            String keyMin = String.valueOf(mpMinLat[0]) + mpMinLon[0] + mpMinLat[1] + mpMinLon[1] + mpMinLat[2] + mpMinLon[2];
            String keyMax = String.valueOf(mpMaxLat[0]) + mpMaxLon[0] + mpMaxLat[1] + mpMaxLon[1] + mpMaxLat[2] + mpMaxLon[2];

            trajectoryInfo.setKeyMin(keyMin);
            trajectoryInfo.setKeyMax(keyMax);
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
