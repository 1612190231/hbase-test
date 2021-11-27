package com.luck.utils;

/**
 * @author luchengkai
 * @description csv文件处理
 * @date 2021/11/25 20:53
 */
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import java.io.IOException;
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
}
