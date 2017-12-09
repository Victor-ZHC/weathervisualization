package com.adc.disasterforecast.tools;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateHelper {

    public static String getPostponeDateByHour(int year, int month, int date, int hourOfDay, int minute, int second, int delayHour) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        Calendar base = Calendar.getInstance();
        base.set(year, month - 1, date, hourOfDay, minute, second);
        base.add(Calendar.HOUR, delayHour);

        return simpleDateFormat.format(base.getTime());
    }

    public static String getPostponeDateByHour(String date, int delayHour) {
        return getPostponeDateByHour(Integer.valueOf(date.substring(0, 4)),
                Integer.valueOf(date.substring(4, 6)),
                Integer.valueOf(date.substring(6, 8)),
                Integer.valueOf(date.substring(8, 10)),
                Integer.valueOf(date.substring(10, 12)),
                Integer.valueOf(date.substring(12, 14)),
                delayHour);
    }

    public static String getPostponeDateByMonth(int year, int month, int date, int hourOfDay, int minute, int second, int delayMonth) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        Calendar base = Calendar.getInstance();
        base.set(year, month - 1, date, hourOfDay, minute, second);
        base.add(Calendar.MONTH, delayMonth);

        return simpleDateFormat.format(base.getTime());
    }

    public static String getPostponeDateByYear(String date, int delayYear) {
        return getPostponeDateByYear(Integer.valueOf(date.substring(0, 4)),
                Integer.valueOf(date.substring(4, 6)),
                Integer.valueOf(date.substring(6, 8)),
                Integer.valueOf(date.substring(8, 10)),
                Integer.valueOf(date.substring(10, 12)),
                Integer.valueOf(date.substring(12, 14)),
                delayYear);
    }

    public static String getPostponeDateByYear(int year, int month, int date, int hourOfDay, int minute, int second, int delayYear) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");

        Calendar base = Calendar.getInstance();
        base.set(year, month - 1, date, hourOfDay, minute, second);
        base.add(Calendar.YEAR, delayYear);

        return simpleDateFormat.format(base.getTime());
    }



    public static String getWarningDate(String date) {
        // 2017-08-04T13:15:00
        String[] parts = date.split("T");
        String[] dates = parts[0].split("-");
        String[] times = parts[1].split(":");
        return dates[1] + "/" + dates[2] + " " + times[0] + ":" + times[1];
    }

    public static String getTimeMillis(String date) {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        Calendar cal = Calendar.getInstance();
        try{
            cal.setTime(simpleDateFormat.parse(date));
        }catch (ParseException e){
            return null;
        }
        return String.valueOf(cal.getTimeInMillis());
    }

    /**
    * @Description 获取时间节点对应的当前小时
    * @Author lilin
    * @Create 2017/12/6 17:58
    **/
    public static String getNowHour(String date) {
        return date.substring(0, 10) + "0000";
    }
    /**
    * @Description 获取当前时间节点的下个小时
    * @Author lilin
    * @Create 2017/12/6 17:57
    **/
    public static String getNextHour(String date) {
        if ("20150616162200".equals(date)) {
            return "20150616170000";
        }
        if ("20150616235300".equals(date)) {
            return "20150617000000";
        }
        if ("20150617020000".equals(date)) {
            return "20150617030000";
        }
        if ("20150617031200".equals(date)) {
            return "20150617040000";
        }
        if ("20150617090000".equals(date)) {
            return "20150617100000";
        }
        return "";
    }

    public static String getNow() {
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
        return simpleDateFormat.format(new Date());
    }

    public static String getFormatWarningMonth(String date, String year) {
        // 2016-08-04T13:15:00
        // return 20170801000000
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");

        Calendar cal = Calendar.getInstance();
        try{
            cal.setTime(simpleDateFormat.parse(date));
        }catch (ParseException e){
            return null;
        }
        cal.set(Calendar.HOUR_OF_DAY, 0);
        cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.DATE, 1);
        cal.set(Calendar.YEAR, Integer.parseInt(year));
        return String.valueOf(cal.getTimeInMillis());
    }

//    public static String getFormatYear(String date, String year) {
//        date = getFormatWarningMonth(date, year);
//
//    }
}
