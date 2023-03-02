package com.luck.utils;


import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

public class TxtUtil {
    public List<String> readTxt(String url) throws IOException {
        List<String> result = new ArrayList<>();
        FileInputStream fin = new FileInputStream(url);
        InputStreamReader reader = new InputStreamReader(fin);
        BufferedReader buffReader = new BufferedReader(reader);
        String strTmp = "";
        while((strTmp = buffReader.readLine())!=null){
            result.add(strTmp);
        }
        buffReader.close();
        return result;
    }
}
