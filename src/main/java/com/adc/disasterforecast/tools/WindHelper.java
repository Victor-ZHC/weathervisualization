package com.adc.disasterforecast.tools;

/**
 * @DESCRIPTION
 *
 * @author lilin
 * @create 2017-11-16 19:42
 **/
public class WindHelper {
//    private static final String levelDesc_4 = "热带低压";
//    private static final String levelDesc_5 = "热带风暴";
//    private static final String levelDesc_6 = "强热带风暴";
//    private static final String levelDesc_7 = "台风";
//    private static final String levelDesc_8 = "强台风";
//    private static final String levelDesc_9 = "超强台风";

    public static String getWindLevel(String windSpeed) {
        float windSpeedNum = Float.parseFloat(windSpeed);
        if (windSpeedNum >= 0.0 && windSpeedNum <= 0.2) {
            return "0";
        }
        if (windSpeedNum >= 0.3 && windSpeedNum <= 1.5) {
            return "1";
        }
        if (windSpeedNum >= 1.6 && windSpeedNum <= 3.3) {
            return "2";
        }
        if (windSpeedNum >= 3.4 && windSpeedNum <= 5.4) {
            return "3";
        }
        if (windSpeedNum >= 5.5 && windSpeedNum <= 7.9) {
            return "4";
        }
        if (windSpeedNum >= 8.0 && windSpeedNum <= 10.7) {
            return "5";
        }
        if (windSpeedNum >= 10.8 && windSpeedNum <= 13.8) {
            return "6";
        }
        if (windSpeedNum >= 13.9 && windSpeedNum <= 17.1) {
            return "7";
        }
        if (windSpeedNum >= 17.2 && windSpeedNum <= 20.7) {
            return "8";
        }
        if (windSpeedNum >= 20.8 && windSpeedNum <= 24.4) {
            return "9";
        }
        if (windSpeedNum >= 24.5 && windSpeedNum <= 28.4) {
            return "10";
        }
        if (windSpeedNum >= 28.5 && windSpeedNum <= 32.6) {
            return "11";
        }
        if (windSpeedNum >= 32.7 && windSpeedNum <= 36.9) {
            return "12";
        }
        if (windSpeedNum >= 37.0 && windSpeedNum <= 41.4) {
            return "13";
        }
        if (windSpeedNum >= 41.5 && windSpeedNum <= 46.1) {
            return "14";
        }
        if (windSpeedNum >= 46.2 && windSpeedNum <= 50.9) {
            return "15";
        }
        if (windSpeedNum >= 51.0 && windSpeedNum <= 56.0) {
            return "16";
        }
        if (windSpeedNum >= 56.1) {
            return "17";
        }
        return "";
    }
}
