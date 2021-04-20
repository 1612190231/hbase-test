package com.luck.test;

import com.luck.service.impl.OperateServiceImpl;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * @author luchengkai
 * @description
 * @date 2021/4/7 22:47
 */
public class test1 {
    public static void test2() throws IOException {
        URL resource = OperateServiceImpl.class.getResource("/hbase-site.xml");
        InputStream inputStream=new FileInputStream(resource.getPath());
        Properties properties=new Properties();
        properties.load(inputStream);
        System.out.println(properties);
    }

    public static void main(String[] args) throws Exception {
        test2();
    }
}
