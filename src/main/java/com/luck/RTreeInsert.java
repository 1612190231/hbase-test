package com.luck;

import com.luck.entity.TrajectoryInfo;
import com.luck.service.HbaseShardService;
import com.luck.service.impl.HbaseShardServiceImpl;
import com.luck.utils.LogUtil;

import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.util.List;

public class RTreeInsert {
    public static void main(String[] args) throws MalformedURLException, ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //获取数据源, 轨迹合并
        HbaseShardService hbaseShardService = new HbaseShardServiceImpl();
//        URL url = new URL("file:////C:\\Users\\user\\Desktop\\code\\hbase-test\\src\\main\\resources\\-1010.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
        URL url = new URL(args[0]);
        List<TrajectoryInfo> trajectoryInfos = hbaseShardService.getTrajectoryInfos(url);
        logUtil.print("load url: " + url);

    }
}
