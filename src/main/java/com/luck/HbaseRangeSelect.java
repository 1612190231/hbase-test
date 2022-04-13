package com.luck;

import com.luck.entity.BaseInfo;
import com.luck.index.XZIndexing;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.locationtech.sfcurve.IndexRange;
import scala.collection.Seq;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class HbaseRangeSelect {

    public static void main(String[] args) throws ParseException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //开始hbase操作
        //初始化
        OperateService operateService = new OperateServiceImpl();
        operateService.setSeries("flow,coordinate,info");
        operateService.setTableName("hbase_shard");
        operateService.init();

        //模糊查询
        //处理range
        String minLon = args[0];
        String maxLon = args[1];
        String minLat = args[2];
        String maxLat = args[3];
//        String minLon = "119.263527";
//        String maxLon = "119.263547";
//        String minLat = "35.157827";
//        String maxLat = "35.157847";
        XZIndexing xzIndexing = new XZIndexing();
        Seq<IndexRange> queryRanges = xzIndexing.ranges((short)9, Double.parseDouble(minLon), Double.parseDouble(minLat),
                Double.parseDouble(maxLon), Double.parseDouble(maxLat));
        List<IndexRange> javaQueryRanges = scala.collection.JavaConversions.seqAsJavaList(queryRanges);
//        String rangeString = String.format("%09d", queryRanges);
//        logUtil.print("range_string: " + rangeString);

        //处理time
        String sTime = args[4];
        String eTime = args[5];
//        String sTime = "29/05/2021 14:00:52";
//        String eTime = "29/05/2021 15:00:52";
        DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        long init_date = df.parse("01/01/2020 00:00:00").getTime();
        int days_s = (int)((df.parse(sTime).getTime() - init_date) / (1000 * 60 * 60 * 24));
        int days_e = (int)((df.parse(eTime).getTime() - init_date) / (1000 * 60 * 60 * 24));
//        logUtil.print("start_time: " + days_s);
//        logUtil.print("start_time: " + days_e);
//        logUtil.print("---------------------");

//        List<String> tmps = new ArrayList<>();
        long startTime=System.currentTimeMillis(); //获取开始时间
        for(int times = days_s; times <= days_e; times++){
            for(IndexRange indexRange : javaQueryRanges){
                int lower = (int)indexRange.lower();
                int upper = (int)indexRange.upper();

                for (int ranges = lower; ranges <= upper; ranges++){
                    String rangeString = String.format("%09d", ranges);
                    String timeString = String.format("%05d", times);
                    String rowKey = timeString + rangeString;
                    ResultScanner rs = operateService.getValueByFilterKey(rowKey);
                    logUtil.getValueByTable(rs);
//                    tmps.add(rowKey);
                }
            }
        }
        long endTime=System.currentTimeMillis(); //获取结束时间
        logUtil.runTimeLog("getValueByTable", endTime, startTime);
    }

}
