package com.adc.disasterforecast.tools;

public class ArrayHelper {
    public static Boolean contains(String[] strings, String string) {
        for (int i = 0; i < strings.length; i++) {
            if (strings[i].equals(string)) {
                return true;
            }
        }
        return false;
    }
}
