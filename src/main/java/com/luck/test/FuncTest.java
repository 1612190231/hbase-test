package com.luck.test;

import com.luck.entity.BaseInfo;
import com.luck.service.OperateService;
import com.luck.service.impl.OperateServiceImpl;
import com.luck.utils.LogUtil;

import java.text.ParseException;
import java.util.*;

public class FuncTest {
    public static void main(String[] args) throws ParseException {
        LogUtil logUtil = new LogUtil();
        OperateService operateService = new OperateServiceImpl();
        operateService.init("test", "data");
        operateService.createTable("test", "data");
        List<BaseInfo> baseInfos = new ArrayList<>();
        Random r = new Random();
        for (int i = 1; i < 10000; i++) {
            BaseInfo baseInfo = new BaseInfo();
            baseInfo.setRowKey(String.valueOf(i));
            baseInfo.setColumnFamilyList(new ArrayList<>(Collections.singleton("data")));

            List<Map<String, Object>> serverColumnsList = new ArrayList<>();
            int finalI = i;
            Map<String, Object> serverColumn = new HashMap<String, Object>(){{
                put("name", "ZhangSan" + finalI);
                put("age", r.nextInt(100));
                put("sex", finalI % 2 == 0 ? "man": "woman");
            }};
            serverColumnsList.add(serverColumn);
            baseInfo.setColumnsList(serverColumnsList);
            baseInfos.add(baseInfo);
        }
        operateService.addByMutator(baseInfos);
    }
}
