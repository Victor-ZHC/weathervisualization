package com.adc.disasterforecast.tools;

/**
 * ${DESCRIPTION}
 *
 * @author lilin
 * @create 2017-11-16 20:38
 **/
public class RainfallHelper {
    private static final String level_1 = "0-50";
    private static final String level_2 = "50-100";
    private static final String level_3 = "100-150";
    private static final String level_4 = "150-200";
    private static final String level_5 = "200";

    public static String getRainfallLevel(String rainfall) {
        float rainfallNum = Float.parseFloat(rainfall);
        if (rainfallNum >= 0 && rainfallNum < 50) {
            return level_1;
        }
        if (rainfallNum >= 50 && rainfallNum < 100) {
            return level_2;
        }
        if (rainfallNum >= 100 && rainfallNum < 150) {
            return level_3;
        }
        if (rainfallNum >= 150 && rainfallNum < 200) {
            return level_4;
        }
        if (rainfallNum >= 200) {
            return level_5;
        }
        return "";
    }
}
