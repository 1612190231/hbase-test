package com.luck.test;

import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.util.*;

public class MergeTest {
    public static void main(String[] args) throws ParseException {
        LogUtil logUtil = new LogUtil();
        OperateService operateService = new OperateServiceImpl();
        operateService.init("test", "data");
        List<RegionInfo> regionInfos = operateService.getRegions();
        try {
            operateService.mergeRegion(Bytes.toString(regionInfos.get(0).getRegionName()), Bytes.toString(regionInfos.get(1).getRegionName()));
        } catch (Exception e){
            logUtil.print(e.toString());
            logUtil.print("merge error!!!");
        }
    }
}
