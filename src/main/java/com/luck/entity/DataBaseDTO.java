package com.luck.entity;

import java.math.BigDecimal;

/**
 * @author luchengkai
 * @description geomesa实体类
 * @date 2021/8/11 19:29
 */
public class DataBaseDTO {
    private String dbName;
    private String ip;
    private String host;

    public String getDbName() {
        return dbName;
    }

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
