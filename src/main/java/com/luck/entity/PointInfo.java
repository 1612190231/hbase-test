package com.luck.entity;

import java.io.Serializable;
import java.util.List;

public class PointInfo implements Serializable {
    private String planNo;
    private String vehicleNo;
    private String utc;
    private double lat;
    private double lon;


    // constructor , getters and setters
    public PointInfo() {
    }

    // constructor , getters and setters
    public PointInfo(String vehicleNo, String utc, double lat, double lon) {
        // TODO Auto-generated constructor stub
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

    public double getLat() {
        return lat;
    }

    public void setLat(double lat) {
        this.lat = lat;
    }

    public double getLon() {
        return lon;
    }

    public void setLon(double lon) {
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
