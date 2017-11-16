package com.adc.disasterforecast.tools;

/**
 * ${DESCRIPTION}
 *
 * @author lilin
 * @create 2017-11-16 21:31
 **/
public class DisasterTypeHelper {
    public static String getRainstormDisasterType(String caseAddr, String caseDesc) {
        String caseStr = caseAddr + caseDesc;
        if (caseStr.contains("房屋") || caseStr.contains("家") || caseStr.contains("幢")) {
            return "房屋进水";
        } else if (caseStr.contains("路口") || caseStr.contains("道路") || caseStr.contains("马路") || caseStr.contains("桥洞")) {
            return "道路积水";
        } else if (caseStr.contains("小区")) {
            return "小区积水";
        } else if (caseStr.contains("车")) {
            return "车辆进水";
        } else if (caseStr.contains("厂区") || caseStr.contains("商铺") || caseStr.contains("厂房")) {
            return "厂区、商铺进水";
        } else {
            return "其他";
        }
    }

    public static String getWindDisasterType(String caseAddr, String caseDesc) {
        String caseStr = caseAddr + caseDesc;
        if (caseStr.contains("道路") || caseStr.contains("树")) {
            return "风灾1";
        } else {
            return "风灾2";
        }
    }
}
