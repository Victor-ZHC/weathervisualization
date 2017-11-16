package com.adc.disasterforecast.tools;

/**
 * ${DESCRIPTION}
 *
 * @author lilin
 * @create 2017-11-16 22:28
 **/
public class WarningHelper {
    public static String getWarningWeather(String type) {
        if ("雷电".equals(type)) {
            return "thunder";
        } else if ("大风".equals(type)) {
            return "wind";
        } else if ("暴雨".equals(type)) {
            return "rain";
        } else {
            return "";
        }
    }

    public static String getWarningLevel(String level) {
        if ("黄色".equals(level)) {
            return "yellow";
        } else if ("蓝色".equals(level)) {
            return "blue";
        } else if ("橙色".equals(level)) {
            return "orange";
        } else if ("红色".equals(level)) {
            return "red";
        } else {
            return "";
        }
    }
}
