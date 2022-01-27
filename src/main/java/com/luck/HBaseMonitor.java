package com.luck;

import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Int;

import java.io.IOException;
import java.util.List;

/**
 * @author luchengkai
 * @description 分区监控
 * @date 2022/1/27 11:09
 */
public class HBaseMonitor {
    public static void main(String[] args) throws IOException {
        /*
         * common args:
         *      monitorType, tableName, downWaterLine, upWaterLine
         * single args:
         *      rowKey
         * all args:
         *      regionLimitCount
         * @author luchengkai
         * @date 2022/1/27 14:22
         * @param args
         * @return void
         */
        // 日志类加载
        LogUtil logUtil = new LogUtil();

        // 参数获取
        String monitorType = args[0];   // 监控类型
        String tableName = args[1];     // 表名
        int downWaterLine = Integer.parseInt(args[2]);  // 下水位线
        int upWaterLine = Integer.parseInt(args[3]);    // 上水位线

        // 初始化操作类
        OperateService operateService = new OperateServiceImpl();
        operateService.init();
//        operateService.setServerNames();
        operateService.setTableName(tableName);

        if (monitorType.equals("single")){
            // 监控计算单个region的数据价值
            String rowKey = args[4];
        }
        else {
            int regionLimitCount = Integer.parseInt(args[4]);   // region限制数
            List<HRegionInfo> regions = operateService.getRegions();  // 指定表的当前region限制数
            if (regions.size() < regionLimitCount) {
                logUtil.print("由于region数: {}未告警，不需要合并", regions.size());
                return;
            }
            regions.sort((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey()));
            for (HRegionInfo hRegionInfo: regions){

            }
        }
    }
}
