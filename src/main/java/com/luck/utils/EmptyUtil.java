package com.luck.utils;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
/**
 * @author luchengkai
 * @description 判空工具类
 * @date 2021/8/12 16:52
 */
public class EmptyUtil {

    /**
     * 判断字符串是否为空，长度为0被认为是空字符串.
     *
     * @param str
     * @return
     */
    public static boolean isEmpty(String str) {
        return str == null || str.length() == 0;
    }

    /**
     * 判断参数是否都为空
     *
     * @param params
     * @return true | false
     */
    public static boolean isAllEmpty(String... params) {
        for (String str : params) {
            if (!isEmpty(str)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断所有参数都不为空
     * @param params
     * @return
     */
    public static boolean isAllNotEmpty(String ... params) {
        for (String str : params) {
            if (isEmpty(str)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 判断列表是否为空，列表没有元素也被认为是空
     *
     *
     * @param collection
     * @return
     */
    public static boolean isEmpty(Collection<?> collection) {
        return collection == null || collection.size() == 0;
    }

    /**
     * 判断数组是否为空
     *
     * @param array
     * @return
     */
    public static boolean isEmpty(Object[] array) {
        return array == null || array.length == 0;
    }

    /**
     * 判断对象是否为空
     *
     * @param obj
     * @return
     */
    public static boolean isEmpty(Object obj) {
        if (obj == null) {
            return true;
        } else {
            return isEmpty(obj.toString());
        }
    }

    /**
     * 判断两个字符串是否一致
     *
     * @param old
     * @param newString
     * @return
     */
    public static boolean isEmpty(String old,String newString) {
        if (!EmptyUtil.isEmpty(old)){
            if (old.equals(newString)){
                return true;
            }else {
                return false;
            }
        }else {
            return false;
        }
    }

    /**
     * 两个字符串 转换成int类型比较大小
     *
     * fanlse 代表 小于  old 小于 new
     * @param old
     * @param newString
     * @return
     */
    public static boolean isSize(String old,String newString) {
        BigInteger oldBig = new BigInteger(old);
        BigInteger newBig = new BigInteger(newString);
        int i = oldBig.compareTo(newBig);
        if (i >= 0){
            return true;
        }
        return false;
    }

    /**
     * 两个字符串 转换成int类型比较大小
     *
     * fanlse 代表 小于  old 小于 new
     * @param old
     * @param startWeight
     * @param endWeight
     * @return
     */
    public static boolean isSize(String old,String startWeight,String endWeight) {
        BigDecimal oldBig = new BigDecimal(old);
        BigDecimal bigstartWeight = new BigDecimal(startWeight);
        BigDecimal bigendWeight = new BigDecimal(endWeight);
        int i = oldBig.compareTo(bigstartWeight); //
        if (i== -1){
            return false;
        }
        int i1 = oldBig.compareTo(bigendWeight);
        if (i1 == 1){
            return false;
        }
        return true;
    }

    /**
     * 字符串 四舍五入
     *
     * @param string
     *
     * @return
     */
    public static String Rounding(String string) {
        double v = Double.parseDouble(string);
        BigDecimal bigDecimal = new BigDecimal(string);
        double v1 = bigDecimal.setScale(2, BigDecimal.ROUND_HALF_UP).doubleValue();
        return v1+"";
    }


    public  static  void main(String[] args){
        boolean size = isSize("0.590000", "0","2");
        System.out.println(size);
    }


}


