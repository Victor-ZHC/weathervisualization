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

}
