package com.luck.tests;

import com.luck.utils.JarUtil;
import org.apache.commons.compress.utils.IOUtils;
import sun.applet.Main;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
//import de.micromata.opengis.kml.v_2_2_0.Kml;
/**
 * @author luchengkai
 * @description jar包解析
 * @date 2021/7/6 11:17
 */
public class JarTest {
    public static void main(String[] args) throws IOException {
//        JarUtil jarUtil = new JarUtil();
//        Object a = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
//        String str = jarUtil.readFile("C:/Users/13908/Desktop/geomesa-hbase-datastore_2.11-3.2.0.jar");
//        System.out.println(str);

        URL url = new URL("jar:file:C:/Users/13908/Desktop/geomesa-hbase-datastore_2.11-3.2.0.jar!/hbase-site.xml");
        InputStream inputStream = url.openStream();
        byte[] bytes = IOUtils.toByteArray(inputStream);
        String content = new String(bytes);
        System.out.println(content);



        /*解析kml文件*/
//        Kml kml = Kml.unmarshal(kmlFile);
//        processKml(kml,typeName);
//
//        /*解析kmz文件*/
//        Kml[] kmls = Kml.unmarshalFromKmz(kmzFile);
//        for(Kml kml : kmls){
//            processKml(kml,typeName);
//        }
    }
}
