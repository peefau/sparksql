package com.inspur.tax.hbase;

import java.util.Calendar;
import java.util.Date;

/**
 * @program: spark_sql_test
 * @description: 处理日期时间的类
 * @author: lipf
 * @create: 2019-03-11 19:04
 **/
public class DateUtils {
    
    public static final String YMD_HMS = "yyyy-MM-dd HH:mm:ss";
    
    public static final String YMD = "yyyyMMdd";
    
    public static final long ONE_DAY_MillIS = 24 * 60 * 60 * 1000;
    
    public static Date now() {
        return new Date();
    }
    
    public static Long currentTimeMillis() {
        return System.currentTimeMillis();
    }
    
    /**
    * @Description: 增加月数
    * @Params:  * @param date
 * @param interval
    * @Return: java.util.Date
    * @Author: lipf
    * @Date: 2019/3/11
    */
    public static Date addMonth(Date date, int interval) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MONTH, interval);
        return calendar.getTime();
    }
    /**
    * @Description: 增加天数
    * @Params:  * @param date
 * @param interval
    * @Return: java.util.Date
    * @Author: lipf
    * @Date: 2019/3/11
    */
    public static Date addDay(Date date, int interval) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.DAY_OF_MONTH, interval);
        return calendar.getTime();
    }
    /**
    * @Description: 增加小时
    * @Params:  * @param date
 * @param interval
    * @Return: java.util.Date
    * @Author: lipf
    * @Date: 2019/3/11
    */ 
    public static Date addHour(Date date, int interval) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.HOUR_OF_DAY, interval);
        return calendar.getTime();
    }
    /**
    * @Description: 增加分钟
    * @Params:  * @param date
 * @param interval
    * @Return: java.util.Date
    * @Author: lipf
    * @Date: 2019/3/11
    */ 
    public static Date addMinute(Date date, int interval) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(date);
        calendar.add(Calendar.MINUTE, interval);
        return calendar.getTime();
    }
    
}
