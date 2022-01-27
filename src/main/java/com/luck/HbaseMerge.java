package com.luck;

import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
/**
 * @author luchengkai
 * @description hbase合区代码
 * @date 2022/1/1 23:05
 */
public class HbaseMerge {

    private Logger logger = LoggerFactory.getLogger(HbaseMerge.class);

    void mergeRegion(TableName tableName, HBaseAdmin admin) throws IOException {
        logger.info("开始合并表【{}】的region", tableName.getNameAsString());

        final List<HRegionInfo> regions = admin.getTableRegions(tableName);

        logger.info("总共有{}个region", regions.size());

        if (regions.size() < 2) {
            admin.close();
            logger.info("由于region数{}太少，并没有合并", regions.size());
            return;
        }
        regions.sort((o1, o2) -> Bytes.compareTo(o1.getStartKey(), o2.getStartKey()));

        for (int i = 0; i < regions.size(); i += 2) {
            HRegionInfo r1 = regions.get(i);
            if (i + 1 >= regions.size()) {
                break;
            }
            final HRegionInfo r2 = regions.get(i + 1);
            logger.info("进度：{}%，开始合并第{}、{}个region：{} 和 {}", (i + 1) * 100.0 / regions.size(), i, i + 1, r1, r2);
            admin.mergeRegions(r1.getEncodedNameAsBytes(), r2.getEncodedNameAsBytes(), false);
        }
    }

}

