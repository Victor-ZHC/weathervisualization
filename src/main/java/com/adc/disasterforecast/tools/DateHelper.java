package com.adc.disasterforecast.tools;

import java.text.SimpleDateFormat;
import java.util.Calendar;

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

    public static String getWarningDate(String date) {
        // 2017-08-04T13:15:00
        String[] parts = date.split("T");
        String[] dates = parts[0].split("-");
        String[] times = parts[1].split(":");
        return dates[1] + "/" + dates[2] + " " + times[0] + ":" + times[1];
    }

}
