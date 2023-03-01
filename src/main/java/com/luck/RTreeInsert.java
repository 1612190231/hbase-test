package com.luck;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import com.github.davidmoten.rtreemulti.internal.EntryDefault;
import com.luck.entity.PointInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.utils.LogUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RTreeInsert {
    public static void main(String[] args) throws MalformedURLException, ParseException, SQLException, ClassNotFoundException {
        //日志类加载
        Class.forName("com.mysql.jdbc.Driver");
        LogUtil logUtil = new LogUtil();

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
        URL url = new URL("file:////E:\\Desktop\\code\\hbase-test\\src\\main\\resources\\test.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
//        URL url = new URL(args[0]);
        List<TrajectoryInfo> trajectoryInfos = hbaseShardService.getTrajectoryInfos(url);
        logUtil.print("load url: " + url);
        PreparedStatement ps;
        PreparedStatement ps1;
        Connection connection = DriverManager.getConnection("jdbc:mysql://114.117.4.200:3306/final_paper?createDatabaseIfNotExist=true&useSSL=false", "luck", "luck");
//        connection.setAutoCommit(false);
        // insert traj
        try {
            int count = 0;
            String sql = "insert into trajInfo values (?,?,?,?,?,?,?,?)";
            ps = connection.prepareStatement(sql);
            for (TrajectoryInfo trajectoryInfo: trajectoryInfos) {
                count++;
                ps.setInt(1, Integer.parseInt(trajectoryInfo.getKeyTime()));
                ps.setString(2, trajectoryInfo.getVehicleNo());
                ps.setDouble(3, trajectoryInfo.getMinLat());
                ps.setDouble(4, trajectoryInfo.getMaxLat());
                ps.setDouble(5, trajectoryInfo.getMinLon());
                ps.setDouble(6, trajectoryInfo.getMaxLon());
                ps.setLong(7, trajectoryInfo.getMinTime());
                ps.setLong(8, trajectoryInfo.getMaxTime());
                ps.addBatch();
                if (count % 1000 == 0) {
                    ps.executeBatch();
                    logUtil.print("trajs commit, count=" + count);
                }
            }
            count = 0;
            String sql1 = "insert into pointInfo values (?,?,?,?,?)";
            ps1 = connection.prepareStatement(sql1);
            for (TrajectoryInfo trajectoryInfo: trajectoryInfos) {
                count++;
                for (PointInfo pointInfo: trajectoryInfo.getPointInfos()) {
                    ps1.setInt(1, Integer.parseInt(trajectoryInfo.getKeyTime()));
                    ps1.setString(2, trajectoryInfo.getVehicleNo());
                    ps1.setString(3, pointInfo.getUtc());
                    ps1.setDouble(4, pointInfo.getLat());
                    ps1.setDouble(5, pointInfo.getLon());
                    ps1.addBatch();
                }
                if (count % 1000 == 0) {
                    ps1.executeBatch();
                    logUtil.print("points commit, count=" + count);
                }
            }
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        // build rtree
        String keyTime = "";
        String sql = "select * from trajInfo";
        Statement st = connection.createStatement();
        ResultSet resultSet = st.executeQuery(sql);
//        List<TrajectoryInfo> trajectoryInfos1 = new ArrayList<>();
        List<Entry<String, Rectangle>> entries = new ArrayList<Entry<String, Rectangle>>(10000);
        Map<String, RTree<String, Rectangle>> treeMap = new HashMap<>();
        while (resultSet.next()) {
            TrajectoryInfo trajectoryInfo = new TrajectoryInfo();
            trajectoryInfo.setKeyTime(String.valueOf(resultSet.getInt(1)));
            trajectoryInfo.setVehicleNo(resultSet.getString(2));
            trajectoryInfo.setMinLat(resultSet.getDouble(3));
            trajectoryInfo.setMaxLat(resultSet.getDouble(4));
            trajectoryInfo.setMinLon(resultSet.getDouble(5));
            trajectoryInfo.setMaxLon(resultSet.getDouble(6));
            trajectoryInfo.setMinTime(resultSet.getLong(7));
            trajectoryInfo.setMaxTime(resultSet.getLong(8));
//            trajectoryInfos1.add(trajectoryInfo);

            if (keyTime.equalsIgnoreCase("")) {
                keyTime = trajectoryInfo.getKeyTime();
                Rectangle rectangle = Rectangle.create(trajectoryInfo.getMinLon(), trajectoryInfo.getMinLat(), trajectoryInfo.getMaxLon(), trajectoryInfo.getMaxLat());
                entries.add(new EntryDefault<String, Rectangle>(trajectoryInfo.getKeyTime() + trajectoryInfo.getVehicleNo(), rectangle));
            }
            else if (!keyTime.equalsIgnoreCase(trajectoryInfo.getKeyTime())){
                RTree<String, Rectangle> rTree = RTree.maxChildren(4).create(entries);
                treeMap.put(trajectoryInfo.getKeyTime(), rTree);
                logUtil.print("build rtree success.");
            }
            else {
                Rectangle rectangle = Rectangle.create(trajectoryInfo.getMinLon(), trajectoryInfo.getMinLat(), trajectoryInfo.getMaxLon(), trajectoryInfo.getMaxLat());
                entries.add(new EntryDefault<String, Rectangle>(trajectoryInfo.getKeyTime() + trajectoryInfo.getVehicleNo(), rectangle));
            }
        }
        logUtil.print("build rtree list success.");
    }
}
