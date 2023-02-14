package com.luck.test;

import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;
import org.apache.hadoop.hbase.client.RegionInfo;
import org.apache.hadoop.hbase.util.Bytes;

import java.text.ParseException;
import java.util.List;

public class BalanceTest {
    public static void main(String[] args) throws ParseException {
        LogUtil logUtil = new LogUtil();
        OperateService operateService = new OperateServiceImpl();
        operateService.init("test", "data");

        try {
            operateService.balanceTable();
        } catch (Exception e){
            logUtil.print(e.toString());
            logUtil.print("balance error!!!");
        }
    }
}
