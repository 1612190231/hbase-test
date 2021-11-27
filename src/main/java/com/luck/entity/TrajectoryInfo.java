package com.luck.entity;

import java.io.Serializable;
import java.util.List;

public class TrajectoryInfo implements Serializable {
    private String planNo;
    private String vehicleNo;
    private String startTime;
    private String endTime;
    private String operationTIme;
    private Float lat;
    private Float lon;

    private List<TrajectoryInfo> trajectoryInfos;
    private List<PointInfo> pointInfos;

    // 分组信息
    private Float minLat;       // 最小纬度
    private Float maxLat;       // 最大纬度
    private Float minLon;       // 最小经度
    private Float maxLon;       // 最大经度
    private Double midLat;       // 中间纬度
    private Double midLon;       // 中间经度
    private String midPoint;     // 中间经纬度列表表示
    private String groupId;      // 分组
    private String keyMin;       // 小索引
    private String keyMax;       // 大索引

    // constructor , getters and setters
    public TrajectoryInfo() {
    }

    // constructor , getters and setters
    public TrajectoryInfo(String planNo, String vehicleNo, String startTime, String endTime, String operationTIme,
                          Float lat, Float lon, Float minLat, Float maxLat, Float minLon, Float maxLon) {
        // TODO Auto-generated constructor stub
        this.planNo = planNo;
        this.vehicleNo = vehicleNo;
        this.startTime = startTime;
        this.endTime = endTime;
        this.operationTIme = operationTIme;
        this.lat = lat;
        this.lon = lon;
        this.minLat = minLat;
        this.maxLat = maxLat;
        this.minLon = minLon;
        this.maxLon = maxLon;
    }

    public String getPlanNo() {
        return planNo;
    }

    public void setPlanNo(String planNo) {
        this.planNo = planNo;
    }

    public String getVehicleNo() {
        return vehicleNo;
    }

    public void setVehicleNo(String vehicleNo) {
        this.vehicleNo = vehicleNo;
    }

    public String getStartTime() {
        return startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public String getOperationTIme() {
        return operationTIme;
    }

    public void setOperationTIme(String operationTIme) {
        this.operationTIme = operationTIme;
    }

    public Float getLat() {
        return lat;
    }

    public void setLat(Float lat) {
        this.lat = lat;
    }

    public Float getLon() {
        return lon;
    }

    public void setLon(Float lon) {
        this.lon = lon;
    }

    public List<TrajectoryInfo> getTrajectoryInfos() {
        return trajectoryInfos;
    }

    public void setTrajectoryInfos(List<TrajectoryInfo> trajectoryInfos) {
        this.trajectoryInfos = trajectoryInfos;
    }

    public Float getMinLat() {
        return minLat;
    }

    public void setMinLat(Float minLat) {
        this.minLat = minLat;
    }

    public Float getMaxLat() {
        return maxLat;
    }

    public void setMaxLat(Float maxLat) {
        this.maxLat = maxLat;
    }

    public Float getMinLon() {
        return minLon;
    }

    public void setMinLon(Float minLon) {
        this.minLon = minLon;
    }

    public Float getMaxLon() {
        return maxLon;
    }

    public void setMaxLon(Float maxLon) {
        this.maxLon = maxLon;
    }

    public Double getMidLat() {
        return midLat;
    }

    public void setMidLat(Double midLat) {
        this.midLat = midLat;
    }

    public Double getMidLon() {
        return midLon;
    }

    public void setMidLon(Double midLon) {
        this.midLon = midLon;
    }

    public String getMidPoint() {
        return midPoint;
    }

    public void setMidPoint(String midPoint) {
        this.midPoint = midPoint;
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId(String groupId) {
        this.groupId = groupId;
    }

    public List<PointInfo> getPointInfos() {
        return pointInfos;
    }

    public void setPointInfos(List<PointInfo> pointInfos) {
        this.pointInfos = pointInfos;
    }

    public String getKeyMin() {
        return keyMin;
    }

    public void setKeyMin(String keyMin) {
        this.keyMin = keyMin;
    }

    public String getKeyMax() {
        return keyMax;
    }

    public void setKeyMax(String keyMax) {
        this.keyMax = keyMax;
    }
}
