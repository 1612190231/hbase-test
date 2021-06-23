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

        SparkConf conf = new SparkConf().setAppName("CSVDemo");
        JavaSparkContext javaSparkContext = new JavaSparkContext(conf);
        SpatialClusterService spatialClusterService= new SpatialClusterServiceImpl();

        JavaRDD<TrajectoryInfo> pointsRdd = spatialClusterService.readCsv(javaSparkContext, args[0]);
        JavaPairRDD<String, TrajectoryInfo> ones = pointsRdd.mapToPair(new PairFunction<TrajectoryInfo, String, TrajectoryInfo>() {
            @Override
            public Tuple2<String, TrajectoryInfo> call(TrajectoryInfo p) {
                System.out.println("plan_no -> " + p.getPlanNo());
                return new Tuple2<String, TrajectoryInfo>(p.getPlanNo(), p);
            }
        });

        JavaPairRDD<String, TrajectoryInfo> results = ones.reduceByKey(new Function2<TrajectoryInfo, TrajectoryInfo, TrajectoryInfo>() {
            @Override
            public TrajectoryInfo call(TrajectoryInfo i1, TrajectoryInfo i2) {
                Float minLat = Math.min(i1.getMinLat(),i2.getMinLat());
                Float maxLat = Math.max(i1.getMaxLat(),i2.getMaxLat());
                Float minLon = Math.min(i1.getMinLon(),i2.getMinLon());
                Float maxLon = Math.max(i1.getMaxLon(),i2.getMaxLon());
                TrajectoryInfo trajectoryInfo = new TrajectoryInfo(i1.getPlanNo(), i1.getVehicleNo(), i1.getStartTime(),
                        i1.getEndTime(), i1.getOperationTIme(), i1.getLat(), i1.getLon(), minLat, maxLat, minLon, maxLon);
                TrajectoryInfo trajectoryInfo1 = new TrajectoryInfo(i2.getPlanNo(), i2.getVehicleNo(), i2.getStartTime(),
                        i2.getEndTime(), i2.getOperationTIme(), i2.getLat(), i2.getLon(), minLat, maxLat, minLon, maxLon);

                List<TrajectoryInfo> trajectoryInfos = i1.getTrajectoryInfos();
                trajectoryInfos.addAll(i2.getTrajectoryInfos());
                trajectoryInfos.add(trajectoryInfo);
                trajectoryInfos.add(trajectoryInfo1);
                trajectoryInfo.setTrajectoryInfos(trajectoryInfos);
                return trajectoryInfo;
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
