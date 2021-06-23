package com.luck.service.impl;

import com.luck.entity.TrajectoryInfo;
import com.luck.service.SpatialClusterService;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class SpatialClusterServiceImpl implements SpatialClusterService, Serializable {
    public JavaRDD<TrajectoryInfo> readCsv(JavaSparkContext javaSparkContext, String path){
        JavaRDD<String> data = javaSparkContext.textFile(path);
        //SQLContext sqlContext = new SQLContext(sc);

        JavaRDD<TrajectoryInfo> rdd_records = data.map(new Function<String, TrajectoryInfo>() {
            public TrajectoryInfo call(String line) throws Exception {
                // Here you can use JSON
                // Gson gson = new Gson();
                // gson.fromJson(line, TrajectoryInfo.class);
                System.out.println("Points -> " + line);
                String[] fields = line.split(",");
                if(fields.length<7 ) return null;
                TrajectoryInfo trajectoryInfo = new TrajectoryInfo(fields[0], fields[1], fields[2], fields[3], fields[4],
                        Float.parseFloat(fields[5]), Float.parseFloat(fields[6]), Float.parseFloat(fields[5]),
                        Float.parseFloat(fields[5]), Float.parseFloat(fields[6]), Float.parseFloat(fields[6]));

                return trajectoryInfo;
            }
        });
//        List<TrajectoryInfo> lr=rdd_records.collect();
//        for(TrajectoryInfo rd:lr){
//            if(rd!=null)
//                System.out.println(rd.area+"|"+rd.orderid+"|"+rd.content+"|"+rd.datetime);
//        }
        //rdd_records.saveAsTextFile("/tmp/10all.txt");
        return rdd_records;
    }
}
