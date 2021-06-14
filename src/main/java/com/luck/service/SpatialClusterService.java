package com.luck.service;

import com.luck.entity.TrajectoryInfo;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public interface SpatialClusterService {
    JavaRDD<TrajectoryInfo> readCsv(JavaSparkContext javaSparkContext, String path);
}
