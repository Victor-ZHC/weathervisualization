package com.adc.disasterforecast.tools;

public class AreaHelper {
    private static String[] district = {"未知", "中心城区", "宝山区", "闵行区", "嘉定区", "青浦区", "金山区", "松江区",
            "奉贤区", "浦东新区", "崇明县", "黄浦区", "静安区", "徐汇区", "杨浦区", "虹口区", "闸北区",
            "长宁区", "普陀区"};
    public static String getDistrictByCode(int code) {
        return district[code];
    }
}
