package com.luck;

import com.github.davidmoten.rtreemulti.Entry;
import com.github.davidmoten.rtreemulti.Iterables;
import com.github.davidmoten.rtreemulti.RTree;
import com.github.davidmoten.rtreemulti.geometry.Rectangle;
import com.github.davidmoten.rtreemulti.internal.EntryDefault;
import com.luck.entity.PointInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.utils.LogUtil;
import com.luck.utils.TxtUtil;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rx.Observable;

public class RTreeSelect {
    public static void main(String[] args) throws IOException, ParseException, SQLException, ClassNotFoundException {
        //日志类加载
        Class.forName("com.mysql.jdbc.Driver");
        LogUtil logUtil = new LogUtil();

        Connection connection = DriverManager.getConnection("jdbc:mysql://114.117.4.200:3306/final_paper?createDatabaseIfNotExist=true&useSSL=false", "luck", "luck");
//        connection.setAutoCommit(false);

        // build rtree
        String keyTime = "";
        String sql = "select keyTime, vehicleNo, minLat, maxLat, minLon, maxLon, minTime, maxTime from trajInfo order by 1";
        Statement st = connection.createStatement();
        ResultSet resultSet = st.executeQuery(sql);
//        List<TrajectoryInfo> trajectoryInfos1 = new ArrayList<>();
        List<Entry<String, Rectangle>> entries = new ArrayList<>(10000);
        Map<Integer, RTree<String, Rectangle>> treeMap = new HashMap<>();
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
            }
            else if (!keyTime.equalsIgnoreCase(trajectoryInfo.getKeyTime())){
                RTree<String, Rectangle> rTree = RTree.maxChildren(4).create(entries);
                treeMap.put(Integer.parseInt(keyTime), rTree);
                entries = new ArrayList<>(10000);
                keyTime = trajectoryInfo.getKeyTime();
//                logUtil.print("build rtree success.");
            }

            Rectangle rectangle = Rectangle.create(trajectoryInfo.getMinLon(), trajectoryInfo.getMinLat(), trajectoryInfo.getMaxLon(), trajectoryInfo.getMaxLat());
            entries.add(new EntryDefault<>(trajectoryInfo.getKeyTime() + trajectoryInfo.getVehicleNo(), rectangle));
        }
        RTree<String, Rectangle> rTree = RTree.maxChildren(4).create(entries);
        treeMap.put(Integer.parseInt(keyTime), rTree);
        logUtil.print("build rtree list success.");
        //处理range

//        URL url = new URL("file:////C:\\Users\\user\\Desktop\\code\\hbase-test\\src\\main\\resources\\-1010.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
//        String url = args[0];
        String url = "E:\\Desktop\\code\\hbase-test\\src\\main\\resources\\query_list.txt";
        TxtUtil txtUtil = new TxtUtil();
        List<String> querys = txtUtil.readTxt(url);
        //模糊查询
        for (String query: querys) {
            int sum = 0;
            long startTime = System.currentTimeMillis(); //获取开始时间
            String[] strs = query.split(",");
            Double minLon = Double.valueOf(strs[0]);
            Double maxLon = Double.valueOf(strs[1]);
            Double minLat = Double.valueOf(strs[2]);
            Double maxLat = Double.valueOf(strs[3]);
            Rectangle qRectangle = Rectangle.create(minLon, minLat, maxLon, maxLat);
            //处理time
            String sTime = strs[4];
            String eTime = strs[5];
            DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            long init_date = df.parse("01/01/2020 00:00:00").getTime();
            int days_s = (int) ((df.parse(sTime).getTime() - init_date) / (1000 * 60 * 60 * 24));
            int days_e = (int) ((df.parse(eTime).getTime() - init_date) / (1000 * 60 * 60 * 24));
            for (int i = days_s; i <= days_e; i++) {
                rTree = treeMap.get(i);
                if (rTree == null) continue;
//                List<Entry<String, Rectangle>> result = Observable.from(rTree.search(qRectangle)).toList().toBlocking().single();
                sum+=Iterables.size(rTree.search(qRectangle));
            }
            long endTime = System.currentTimeMillis(); //获取结束时间
            logUtil.print("data sum: " + sum);
            logUtil.runTimeLog("getValueByTable", endTime, startTime);
        }
    }
}
