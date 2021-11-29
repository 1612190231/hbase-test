package com.luck.test;

import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Locale;

/**
 * @author luchengkai
 * @description 测试
 * @date 2021/9/10 11:09
 */
public class test {
    public static void main(String[] args) {
        DateTimeFormatter dateFormat = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss", Locale.CHINA);
        Object value = Date.from(LocalDate.parse("2021/08/01 07:12:40", dateFormat).atStartOfDay(ZoneOffset.UTC).toInstant());
        System.out.println(value);
    }
}
