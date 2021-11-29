package com.luck.test;

import com.luck.entity.TrajectoryInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author luchengkai
 * @description reduce合并测试
 * @date 2021/6/24 12:49
 */


public class ReduceTest {
    public static TrajectoryInfo call(TrajectoryInfo i1, TrajectoryInfo i2) {
        double minLat = Math.min(i1.getMinLat(),i2.getMinLat());
        double maxLat = Math.max(i1.getMaxLat(),i2.getMaxLat());
        double minLon = Math.min(i1.getMinLon(),i2.getMinLon());
        double maxLon = Math.max(i1.getMaxLon(),i2.getMaxLon());
        TrajectoryInfo trajectoryInfo1 = new TrajectoryInfo(i1.getPlanNo(), i1.getVehicleNo(), i1.getStartTime(),
                i1.getEndTime(), i1.getOperationTIme(), i1.getLat(), i1.getLon(), minLat, maxLat, minLon, maxLon);
        TrajectoryInfo trajectoryInfo2 = new TrajectoryInfo(i2.getPlanNo(), i2.getVehicleNo(), i2.getStartTime(),
                i2.getEndTime(), i2.getOperationTIme(), i2.getLat(), i2.getLon(), minLat, maxLat, minLon, maxLon);

        List<TrajectoryInfo> trajectoryInfos = new ArrayList<>();
        if (i1.getTrajectoryInfos() != null){
            trajectoryInfos.addAll(i1.getTrajectoryInfos());
        }
        if (i2.getTrajectoryInfos() != null) {
            trajectoryInfos.addAll(i2.getTrajectoryInfos());
        }
        trajectoryInfos.add(trajectoryInfo1);
        trajectoryInfos.add(trajectoryInfo2);
        trajectoryInfo1.setTrajectoryInfos(trajectoryInfos);
        return trajectoryInfo1;
    }

    public static void main(String[] args) throws Exception {
        TrajectoryInfo trajectoryInfo = new TrajectoryInfo("DD210305000182", "鲁LD7167",
                "2021/3/5 6:28","2021/3/5 8:49", "2021/3/5 6:40",
                Float.parseFloat("35.3625"), Float.parseFloat("119.449545"), Double.parseDouble("35.3625"),
                Double.parseDouble("35.3625"), Double.parseDouble("119.449545"), Double.parseDouble("119.449545"));
        TrajectoryInfo trajectoryInfo2 = new TrajectoryInfo("DD210305000182", "鲁LD7167",
                "2021/3/5 6:28","2021/3/5 8:49", "2021/3/5 6:41",
                Float.parseFloat("35.36233"),Float.parseFloat("119.4494067"), Double.parseDouble("35.36233"),
                Double.parseDouble("35.36233"), Double.parseDouble("119.4494067"), Double.parseDouble("119.4494067"));
        TrajectoryInfo trajectoryInfo1 = call(trajectoryInfo, trajectoryInfo2);
        System.out.println(trajectoryInfo1);
    }
}
