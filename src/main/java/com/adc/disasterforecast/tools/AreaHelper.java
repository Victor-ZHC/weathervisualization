package com.adc.disasterforecast.tools;

import java.util.HashMap;
import java.util.Map;

public class AreaHelper {

    public static final Double SHANGHAI_LAT_MIN = 30.67;
    public static final Double SHANGHAI_LAT_MAX = 31.84;
    public static final Double SHANGHAI_LON_MIN = 120.85;
    public static final Double SHANGHAI_LON_MAX = 122.20;
    private static String[] district = {"未知", "中心城区", "宝山区", "闵行区", "嘉定区", "青浦区", "金山区", "松江区",
            "奉贤区", "浦东新区", "崇明县", "黄浦区", "静安区", "徐汇区", "杨浦区", "虹口区", "闸北区",
            "长宁区", "普陀区"};
    public static String getDistrictByCode(int code) {
        return district[code];
    }

    public static Map<String, Integer> getAreaMap() {
        Map<String, Integer> areaMap = new HashMap<>();
        for (int i = 0; i < district.length; i++) {
            areaMap.put(district[i], 0);
        }
        return areaMap;
    }
}
