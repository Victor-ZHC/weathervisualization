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
        if (rainfall.equals("")) {
            return "0";
        }
        double rainfallNum = Double.parseDouble(rainfall);
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

    public static String getRainfallColor(String rainfall) {
        String level = getRainfallLevel(rainfall);

        switch (level) {
            case level_1: return "normal";
            case level_2: return "blue";
            case level_3: return "yellow";
            case level_4: return "orange";
            case level_5: return "red";
            default: return "";
        }
    }

    public static double getRainHour(String rainHour) {
        if (rainHour.equals("")) {
            return 0.0;
        }

        double rain = Double.valueOf(rainHour);
        return rain < 0 ? 0 : rain;
    }
}
