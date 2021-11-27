package com.luck.enums;

/**
 * @author luchengkai
 * @description 文件大小静态变量
 * @date 2021/8/12 17:07
 */
public enum SizeUnitEnum {
    NOT_LOADING("SIZETYPE_B",1),
    HAS_LOADING("SIZETYPE_KB",2),
    ASSIGNED_TEAM("SIZETYPE_MB", 3),
    SPIDERED("SIZETYPE_GB", 4),

    ;
    private String code;
    private int value;

    private SizeUnitEnum(String code, int value) {
        this.code = code;
        this.value = value;
    }

    public static int getValue(String code) {
        for (SizeUnitEnum obj : SizeUnitEnum.values()) {
            if (obj.code.equals(code)) {
                return obj.value;
            }
        }
        return -1;
    }

    public String getCode() {
        return code;
    }

    public int getValue() {
        return value;
    }

}
