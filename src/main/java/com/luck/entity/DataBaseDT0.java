package com.luck.entity;

import java.math.BigDecimal;

/**
 * @author luchengkai
 * @description geomesa实体类
 * @date 2021/8/11 19:29
 */
public class DataBaseDT0 {
    private String dbName;
    private String ip;
    private String host;
    private String orderNoRobbed;
    private String orderType;
    private String consignorCode;
    private String consignorName;
    private String carrierCode;
    private String carrierName;
    private String status;
    private String validStatus;
    private Double planTotalWeight;
    private Double planTotalNo;
    private String carrierType;
    private String carrierSplit;
    private String driverType;
    private String driverSplit;
    private BigDecimal ceilingPriceTax;
    private BigDecimal ceilingPriceTaxNo;
    private BigDecimal lowerPriceTax;//价格下限（含税）
    private BigDecimal lowerPriceTaxNo;//价格下限（不含税）
    private String bidTimeStart;
    private String bidTimeEnd;
    private String bidStatus;
    private String publishCode;
    private String publishName;
    private String publishTime;
    private String evaluationCode;
    private String evaluationName;
    private String evaluationTime;
    private Integer rebidType;
    private String remark;
    private String createId;
    private String createDate;
    private String updateId;
    private String updateDate;
    private double outTotalWeight;
    private double outTotalNo;
    private double actTotalWeight;
    private double actTotalNo;
}
