package com.luck;

import com.luck.index.XZIndexing;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.locationtech.sfcurve.IndexRange;
import scala.collection.Seq;

import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class JustSelect {

    public static void main(String[] args) throws ParseException, IOException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //开始hbase操作
        //初始化
        OperateService operateService = new OperateServiceImpl();
        operateService.setSeries("data");
        operateService.setTableName("track_just");
        operateService.init();

        //模糊查询
        //处理range
        String minLon = args[0];
        String maxLon = args[1];
        String minLat = args[2];
        String maxLat = args[3];
        XZIndexing xzIndexing = new XZIndexing();
        Seq<IndexRange> queryRanges = xzIndexing.ranges((short)9, Double.parseDouble(minLon), Double.parseDouble(minLat),
                Double.parseDouble(maxLon), Double.parseDouble(maxLat));
        List<IndexRange> javaQueryRanges = scala.collection.JavaConversions.seqAsJavaList(queryRanges);

        //处理time
        String sTime = args[4];
        String eTime = args[5];
        DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
        long init_date = df.parse("01/01/2020 00:00:00").getTime();
        int days_s = (int)((df.parse(sTime).getTime() - init_date) / (1000 * 60 * 60 * 24));
        int days_e = (int)((df.parse(eTime).getTime() - init_date) / (1000 * 60 * 60 * 24));

        int sum = 0;
        long startTime=System.currentTimeMillis(); //获取开始时间
        for(int times = days_s; times <= days_e; times++){
            for(IndexRange indexRange : javaQueryRanges){
                int lower = (int)indexRange.lower();
                int upper = (int)indexRange.upper();

                for (int ranges = lower; ranges <= upper; ranges++){
                    String rangeString = String.format("%09d", ranges);
                    String timeString = String.format("%05d", times);
                    String rowKey = timeString + rangeString;
                    Filter filter = new RowFilter(CompareFilter.CompareOp.EQUAL, new SubstringComparator(rowKey));
                    ResultScanner rs = operateService.getByFilter(filter);
//                    logUtil.getValueByTable(rs);
//                    tmps.add(rowKey);
                    sum++;
                }
            }
        }
        long endTime=System.currentTimeMillis(); //获取结束时间
        logUtil.print("data sum: " + sum);
        logUtil.runTimeLog("getValueByTable", endTime, startTime);
        long selectTime = endTime - startTime;

//        DealService dealService = new DealServiceImpl();
//        dealService.dealSelectTime(operateService, selectTime);

    }

}
