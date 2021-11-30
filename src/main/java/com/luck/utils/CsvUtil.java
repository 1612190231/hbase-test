package com.luck.utils;

/**
 * @author luchengkai
 * @description csv文件处理
 * @date 2021/11/25 20:53
 */
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.*;


public class CsvUtil {

    /**
     * 解析csv文件
     *
     * @param url csv文件
     * @return 数组
     */
    public List<CSVRecord> readCsv(URL url) {
        List<CSVRecord> result = new ArrayList<>();
        Map<String, Object> cell = new HashMap<String, Object>();
        try (CSVParser parser = CSVParser.parse(url, StandardCharsets.UTF_8, CSVFormat.DEFAULT)) {
            for (CSVRecord record : parser) {
                try {
                    result.add(record);
                } catch (Exception e) {
                    System.out.println("Invalid GDELT record: " + e.toString() + " " + record.toString());
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error reading GDELT data:", e);
        }
        return result;
    }

    /**
     * 读取
     * @param file csv文件(路径+文件名)，csv文件不存在会自动创建
     * @param dataList 数据
     * @return
     */
    public static boolean exportCsv(File file, List<String> dataList){
        boolean isSucess=false;

        FileOutputStream out=null;
        OutputStreamWriter osw=null;
        BufferedWriter bw=null;
        try {
            out = new FileOutputStream(file);
            osw = new OutputStreamWriter(out);
            bw =new BufferedWriter(osw);
            if(dataList!=null && !dataList.isEmpty()){
                for(String data : dataList){
                    bw.append(data).append("\r");
                }
            }
            isSucess=true;
        } catch (Exception e) {
            isSucess=false;
        }finally{
            if(bw!=null){
                try {
                    bw.close();
                    bw=null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(osw!=null){
                try {
                    osw.close();
                    osw=null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            if(out!=null){
                try {
                    out.close();
                    out=null;
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        return isSucess;
    }
}
