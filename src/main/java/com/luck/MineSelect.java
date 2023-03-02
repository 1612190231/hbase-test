package com.luck;

import com.luck.index.XZIndexing;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import com.luck.utils.ThreadUtil;
import com.luck.utils.TxtUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.locationtech.sfcurve.IndexRange;
import scala.collection.Seq;

import java.io.IOException;
import java.net.URL;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * @author luchengkai
 * @description hbase-test主类
 * @date 2021/4/17 14:36
 */
public class MineSelect {

    public static void main(String[] args) throws ParseException, IOException {
        //日志类加载
        LogUtil logUtil = new LogUtil();

        //初始化
        OperateService operateService = new OperateServiceImpl();
        operateService.init("track_mine", "data");

//        URL url = new URL("file:////C:\\Users\\user\\Desktop\\code\\hbase-test\\src\\main\\resources\\-1010.csv");
//        URL url = new URL("file:////root/data/test/data/-210830.csv");
        String url = args[0];
        TxtUtil txtUtil = new TxtUtil();
        List<String> querys = txtUtil.readTxt(url);
        //模糊查询
        for (String query: querys) {
            String[] strs = query.split(",");
            //模糊查询
            //处理range
            String minLon = strs[0];
            String maxLon = strs[1];
            String minLat = strs[2];
            String maxLat = strs[3];
            XZIndexing xzIndexing = new XZIndexing();
            Seq<IndexRange> queryRanges = xzIndexing.ranges((short) 9, Double.parseDouble(minLon), Double.parseDouble(minLat),
                    Double.parseDouble(maxLon), Double.parseDouble(maxLat));
            List<IndexRange> javaQueryRanges = scala.collection.JavaConversions.seqAsJavaList(queryRanges);

            //处理time
            String sTime = strs[4];
            String eTime = strs[5];
            DateFormat df = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss");
            long init_date = df.parse("01/01/2020 00:00:00").getTime();
            int days_s = (int) ((df.parse(sTime).getTime() - init_date) / (1000 * 60 * 60 * 24));
            int days_e = (int) ((df.parse(eTime).getTime() - init_date) / (1000 * 60 * 60 * 24));

            int sum = 0;
            long startTime = System.currentTimeMillis(); //获取开始时间
            List<String> rowKeys = new ArrayList<>();
            for (int times = days_s; times <= days_e; times++) {
                for (IndexRange indexRange : javaQueryRanges) {
                    int lower = (int) indexRange.lower();
                    int upper = (int) indexRange.upper();

                    for (int ranges = lower; ranges <= upper; ranges++) {
                        String rangeString = String.format("%09d", ranges);
                        String timeString = String.format("%05d", times);
                        String rowKey = timeString + rangeString;
                        //                    String valueRowkey = "";
                        PrefixFilter filter = new PrefixFilter(rowKey.getBytes());
                        ResultScanner rs = operateService.getByFilter(filter);
                        boolean flag = false;
                        for (Result result : rs) {
                            List<Cell> cells = result.listCells();
                            for (Cell cell : cells) {
                                try {
                                    //                                valueRowkey = Bytes.toString(cell.getRowArray(),cell.getRowOffset(),cell.getRowLength()); // 获取rowkey
                                    String familyName = Bytes.toString(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyLength()); // 获取列族名
                                    String columnName = Bytes.toString(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierLength()); // 获取列名
                                    String value = Bytes.toString(cell.getValueArray(), cell.getValueOffset(), cell.getValueLength());
                                    //                                logUtil.print("数据的rowkey为" + rowkey + "列族名为" + familyName + "列名为" + columnName + "列的值为" + value);
                                    sum++;
                                    flag = true;
                                } catch (Exception e) {
                                    logUtil.print(e.toString());
                                }
                            }
                        }
                        if (flag) rowKeys.add(rowKey);
                    }
                }
            }
            long endTime = System.currentTimeMillis(); //获取结束时间
            logUtil.print("data sum: " + sum);
            logUtil.runTimeLog("getValueByTable", endTime, startTime);

            ThreadUtil threadUtil = new ThreadUtil(rowKeys, endTime - startTime);
            threadUtil.start();
        }
    }

}
