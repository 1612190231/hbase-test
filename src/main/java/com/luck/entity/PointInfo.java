package com.luck.entity;

import java.io.Serializable;
import java.util.List;

public class PointInfo implements Serializable {
    private String planNo;
    private String vehicleNo;
    private String utc;
    private Float lat;
    private Float lon;


    // constructor , getters and setters
    public PointInfo() {
    }

    // constructor , getters and setters
    public PointInfo(String planNo, String vehicleNo, String utc, Float lat, Float lon) {
        // TODO Auto-generated constructor stub
        this.planNo = planNo;
        this.vehicleNo = vehicleNo;
        this.utc = utc;
        this.lat = lat;
        this.lon = lon;
    }

    public String getPlanNo() {
        return planNo;
    }

    public void setPlanNo(String planNo) {
        this.planNo = planNo;
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

    public String getVehicleNo() {
        return vehicleNo;
    }

    public void setVehicleNo(String truckNo) {
        this.vehicleNo = truckNo;
    }

    public String getUtc() {
        return utc;
    }

    public void setUtc(String utc) {
        this.utc = utc;
    }
}
