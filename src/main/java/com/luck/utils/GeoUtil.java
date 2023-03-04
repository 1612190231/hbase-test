package com.luck.utils;

import org.gavaghan.geodesy.Ellipsoid;
import org.gavaghan.geodesy.GeodeticCalculator;
import org.gavaghan.geodesy.GeodeticCurve;
import org.gavaghan.geodesy.GlobalCoordinates;

import java.math.BigDecimal;
import java.math.RoundingMode;

public class GeoUtil {

    public static void main(String[] args) {
        //计算两点坐标的距离
//        getDistance(114.008919919000230, 22.727150549443284, 114.008919967000230, 22.727150537443284,3);
        //已知起点坐标、角度方向、距离(示例3.2mm)，计算另一个坐标的经纬度
        getGlobalCoordinates(119.263527, 35.157827, 0, 20000);
    }


    /**
     * 根据经纬度，计算两点间的距离、方位、反方位
     * @param longitudeFrom  第一个点的经度
     * @param latitudeFrom  第一个点的纬度
     * @param longitudeTo 第二个点的经度
     * @param latitudeTo  第二个点的纬度
     * @param accurate  保留小数点几位
     */
    public static void getDistance(double longitudeFrom, double latitudeFrom, double longitudeTo, double latitudeTo,int accurate) {
        GlobalCoordinates source = new GlobalCoordinates(latitudeFrom, longitudeFrom);
        GlobalCoordinates target = new GlobalCoordinates(latitudeTo, longitudeTo);
        //创建GeodeticCalculator，调用计算方法，传入坐标系、经纬度得到GeodeticCurve，用GeodeticCurve获取距离、方位、反方位
        GeodeticCurve geodeticCurve = new GeodeticCalculator().calculateGeodeticCurve(Ellipsoid.WGS84, source, target);
        //获取两点的方位
        double azimuth = geodeticCurve.getAzimuth();
        //获取两点之间的距离
        double distance = geodeticCurve.getEllipsoidalDistance();
        double v = distance*1000;//距离转为毫米
        //保留数据小数点位数且四舍五入
        BigDecimal bigDecimal = new BigDecimal(v).setScale(accurate, RoundingMode.HALF_UP);
        double result = bigDecimal.doubleValue();
        System.out.println("两个坐标之间的距离是"+ result + "mm");
        System.out.println("两个坐标的方向是"+azimuth);
    }



    /**
     * 根据开始坐标点，角度，计算结束点坐标
     * @param longitudeFrom 开始点经度
     * @param latitudeFrom  开始点纬度
     * @param startAngle 方向（以起点为中心）如果是90角，startAngle=90，表示纬度不变，经度向东移动增大
     *                   这里的角度方向就是上图中的a角，正北方向是0，正东方向是90
     * @param distance 距离（单位：m）
     */
    public static void getGlobalCoordinates(double longitudeFrom, double latitudeFrom, double startAngle, double distance){
        //经纬度对象
        GlobalCoordinates startGlobalCoordinates = new GlobalCoordinates(latitudeFrom, longitudeFrom);
        //计算的坐标对象
        GlobalCoordinates globalCoordinates = new GeodeticCalculator().calculateEndingGlobalCoordinates(Ellipsoid.WGS84, startGlobalCoordinates, startAngle, distance);
        //获取纬度
        double latitude = globalCoordinates.getLatitude();
        //获取经度
        double longitude = globalCoordinates.getLongitude();
        System.out.println("坐标经度="+longitude+",坐标纬度="+latitude);
    }

}


