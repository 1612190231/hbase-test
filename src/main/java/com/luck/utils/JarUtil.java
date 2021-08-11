package com.luck.utils;

import org.apache.log4j.Logger;
import sun.applet.Main;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.jar.JarFile;
import java.util.zip.ZipEntry;

/**
 * @author luchengkai
 * @description jar包解析
 * @date 2021/7/6 11:11
 */
public class JarUtil {
    private Logger logger = Logger.getLogger(this.getClass());

    public String readFile(String file) throws IOException {
        Object a = Main.class.getProtectionDomain().getCodeSource();
        String jarPath = Main.class.getProtectionDomain().getCodeSource().getLocation().getPath();
        InputStream input = null;
        BufferedInputStream bis = null;
        StringBuilder sb = new StringBuilder();
        try {
            JarFile jarFile = new JarFile(jarPath);
            ZipEntry zipEntry = jarFile.getEntry(file);
            //can not find the deserved file
            if (zipEntry == null) {
                logger.info("Can not find file");
                return null;
            }

            input = jarFile.getInputStream(zipEntry);
            bis = new BufferedInputStream(input);

            byte[] temp = new byte[1024];
            int len;
            while ((len = bis.read(temp)) != -1) {
                sb.append(new String(temp, 0, len, StandardCharsets.UTF_8));
            }
        } catch (IOException ex) {
            logger.error("Fail to read file");
            logger.error("Exception:", ex);
            throw new RuntimeException("A valid file " + file + " is unavailable.");
        } finally {
            if (bis != null) {
                bis.close();
            }

            if (input != null) {
                input.close();
            }
        }

        return sb.toString();
    }
}
