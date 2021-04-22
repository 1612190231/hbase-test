package com.luck.utils;

import org.apache.log4j.Logger;

import java.util.List;

/**
 * @author luchengkai
 * @description 日志处理类
 * @date 2021/4/22 16:29
 */
public class LogUtil {
    private Logger logger = Logger.getLogger(this.getClass());
    public void runTimeLog(String type, long endTime, long startTime){
        logger.info(type + " project runtime: "+(endTime-startTime)+"ms");
    }

    public void prepareLog(List list){
        String info = String.join(",", list);
        logger.info("excelData: " + info);
    }
}
