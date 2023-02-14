package com.luck.test;

import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class SplitTest {
    public static void main(String[] args) throws ParseException, IOException {
        LogUtil logUtil = new LogUtil();
        OperateService operateService = new OperateServiceImpl();
        operateService.init("test", "data");
        List<RegionInfo> regionInfos = operateService.getRegions();
        try {
            operateService.splitRegion(Bytes.toString(regionInfos.get(0).getRegionName()));
        } catch (Exception e){
            logUtil.print(e.toString());
            logUtil.print("split error!!!");
        }

    }
}
