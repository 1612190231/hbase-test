package com.luck.service;

import com.luck.entity.PointInfo;
import com.luck.entity.TrajectoryInfo;

import java.io.File;
import java.net.URL;
import java.text.ParseException;
import java.util.List;

/**
 * @author luchengkai
 * @description 分区服务
 * @date 2021/11/26 1:36
 */
public interface HbaseShardService {
    List<TrajectoryInfo> getTrajectoryInfos(URL url) throws ParseException;

    List<TrajectoryInfo> constructMbr(List<TrajectoryInfo> trajectoryInfos) throws ParseException;

    List<TrajectoryInfo> dimensionReduction(List<TrajectoryInfo> trajectoryInfos);
}
