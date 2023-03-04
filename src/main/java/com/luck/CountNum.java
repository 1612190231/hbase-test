package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.entity.KeyInfo;
import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.OperateService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.ByteUtil;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.*;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class CountNum {

    public static void main(String[] args) throws MalformedURLException, ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
        URL url = new URL("file:////D:\\code\\hbase-test\\src\\main\\resources\\data\\-220430.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
//        URL url = new URL(args[0]);
        List<TrajectoryInfo> trajectoryInfos = hbaseShardService.getTrajectoryInfos(url);
        logUtil.print("load url: " + url);
        logUtil.print("trajectories count: " + trajectoryInfos.size());
        int pointCount = 0;
        for (TrajectoryInfo trajectoryInfo: trajectoryInfos) {
            pointCount += trajectoryInfo.getPointInfos().size();
        }
        logUtil.print("points count: "+ pointCount);
    }
}
