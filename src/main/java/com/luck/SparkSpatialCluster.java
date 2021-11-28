package com.luck;

/**
 * User: hadoop
 * Date: 2014/10/10 0010
 * Time: 19:26
 */

import com.luck.entity.TrajectoryInfo;
import com.luck.service.SpatialClusterService;
import com.luck.service.impl.SpatialClusterServiceImpl;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public final class SparkSpatialCluster {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        if (args.length < 1) {
            System.err.println("Usage: JavaSpatialCluster <file>");
            System.exit(1);
        }

//        System.out.println("112312\n");
        SparkConf conf = new SparkConf().setAppName("CSVDemo");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
//        SpatialClusterService spatialClusterService= new SpatialClusterServiceImpl();
//        JavaRDD<TrajectoryInfo> pointsRdd = spatialClusterService.readCsv(javaSparkContext, args[0]);
        JavaRDD<String> lines = javaSparkContext.textFile(args[0]);
//        System.out.println("---------------lines start----------------");
//        System.out.println(lines.collect());
//        System.out.println("--------------- lines end-----------------");
        //SQLContext sqlContext = new SQLContext(sc);

        // 提取轨迹点数据，并转成RDD结构
        JavaRDD<TrajectoryInfo> pointsRdd = lines.map(new Function<String, TrajectoryInfo>() {
            public TrajectoryInfo call(String line) throws Exception {
//                System.out.println("---------------line start----------------");
//                System.out.println("Points -> " + line);
//                System.out.println("--------------- line end-----------------");
                String[] fields = line.split("\t");
                if(fields.length<7 ) return null;
                TrajectoryInfo trajectoryInfo = new TrajectoryInfo(fields[0], fields[1], fields[2], fields[3], fields[4],
                        Float.parseFloat(fields[5]), Float.parseFloat(fields[6]), Double.parseDouble(fields[5]),
                        Double.parseDouble(fields[5]), Double.parseDouble(fields[6]), Double.parseDouble(fields[6]));

                return trajectoryInfo;
            }
        });
//        System.out.println("---------------points start----------------");
//        System.out.println(pointsRdd.collect());
//        System.out.println("--------------- points end-----------------");
        // mapToPair算子
        JavaPairRDD<String, TrajectoryInfo> ones = pointsRdd.mapToPair(new PairFunction<TrajectoryInfo, String, TrajectoryInfo>() {
            @Override
            public Tuple2<String, TrajectoryInfo> call(TrajectoryInfo p) {
//                System.out.println("plan_no -> " + p.getPlanNo());
                return new Tuple2<String, TrajectoryInfo>(p.getPlanNo(), p);
            }
        });

//        System.out.println("---------------points start----------------");
//        List<Tuple2<String, TrajectoryInfo>> tmp_ones = ones.collect();
//        for (Tuple2<?, ?> tuple : tmp_ones) {
//            System.out.println("plan_no -> " + tuple._1());
//        }
//        System.out.println("--------------- points end-----------------");

        // reduceByKey算子
        JavaPairRDD<String, TrajectoryInfo> results = ones.reduceByKey(new Function2<TrajectoryInfo, TrajectoryInfo, TrajectoryInfo>() {
            @Override
            public TrajectoryInfo call(TrajectoryInfo i1, TrajectoryInfo i2) {
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
        });

        List<Tuple2<String, TrajectoryInfo>> output = results.collect();
        for (Tuple2<?, ?> tuple : output) {
            System.out.println(tuple._1() + ": " + tuple._2());
        }
        results.map(new Function<Tuple2<String, TrajectoryInfo>, String>() {
            @Override
            public String call(Tuple2<String, TrajectoryInfo> arg0) throws Exception {
                return arg0._1.toUpperCase() + ": " + arg0._2;
            }
        }).saveAsTextFile(args[1]);

        javaSparkContext.stop();
    }
}
