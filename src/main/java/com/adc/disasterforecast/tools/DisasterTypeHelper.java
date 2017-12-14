package com.adc.disasterforecast.tools;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ${DESCRIPTION}
 *
 * @author lilin
 * @create 2017-11-16 21:31
 **/
public class DisasterTypeHelper {

    public static final String[] CODE_DISASTER = {"非灾害", "暴雨", "大风", "雷电", "高温", "暴雪", "道路结冰", "大雾", "冰雹", "霜冻", "霾", "寒潮"};
    public static long DISASTER_RAIN_CODE = 1;
    public static long DISASTER_WIND_CODE = 2;
    public static long DISASTER_THUNDER_CODE = 3;
    public static long DISTRICT_CODE = 13;
    private static String[] disasterType = {"rain", "wind", "thunder"};

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
        if (caseStr.contains("树")) {
            return "树木倒伏";
        } else if (caseStr.contains("广告") || caseStr.contains("牌")) {
            return "广告牌受损";
        } else if (caseStr.contains("房屋") || caseStr.contains("家") || caseStr.contains("幢")) {
            return "房屋受损";
        } else if (caseStr.contains("电线")) {
            return "电线断裂";
        } else if (caseStr.contains("信号灯") || caseStr.contains("红绿灯") || caseStr.contains("指示灯")) {
            return "信号灯受损";
        } else {
            return "构筑物受损";
        }
    }

    public static JSONObject getAreaDisasterType(String area, List<JSONObject> disasters) {

        JSONArray disasterTypeList = getDisasterTypeInJsonArray(disasters);

        JSONObject areaDisasterType = new JSONObject();
        areaDisasterType.put("area", area);
        areaDisasterType.put("value", disasterTypeList);

        return areaDisasterType;
    }

    public static String getDisasterTypeByCode(int disasterCode) {
        switch (disasterCode) {
                case 1: return "rain";
                case 2: return "wind";
                case 3: return "thunder";
                default: return "";
        }
    }

    public static String getDisasterType(String desc) {
        if (desc.contains("高空")) {
            return "高空坠物";
        } else if (desc.contains("房屋进水")) {
            return "房屋进水";
        } else if (desc.contains("农田")) {
            return "农田进水";
        } else {
            return "其他";
        }
    }

    public static Map<String, Integer> getDisasterMap() {
        Map<String, Integer> disasterMap = new HashMap<>();
        for (int i = 0; i < disasterType.length; i++) {
            disasterMap.put(disasterType[i], 0);
        }
        return disasterMap;
    }

    public static JSONArray getDisasterTypeInJsonArray(List<JSONObject> disasters) {
        int[] disasterType = new int[10];
        String[] disasterTypeName = {"树倒", "房屋进水", "工商业区域进水", "车辆受损", "交通受阻", "电力系统受损", "农作区进水", "建筑受损", "小区进水", "其他"};

        for (JSONObject disaster : disasters) {
            String code = (String) disaster.get("Disaster_Code");
            if ("2".equals(code)) {
                disasterType[0] += 1;
            } else {
                String description = (String) disaster.get("Disaster_Description");
                if (description.contains("室进水") || description.contains("漏水")) {
                    disasterType[1] += 1;
                } else if (description.contains("商场") || description.contains("厂房") || description.contains("仓库") || description.contains("店面") || description.contains("门面")) {
                    disasterType[2] += 1;
                } else if (description.contains("车辆")) {
                    disasterType[3] += 1;
                } else if (description.contains("堵塞")) {
                    disasterType[4] += 1;
                } else if (description.contains("漏电") || description.contains("断电")) {
                    disasterType[5] += 1;
                } else if (description.contains("农田") || description.contains("池塘") || description.contains("鱼塘")){
                    disasterType[6] += 1;
                } else if (description.contains("房屋") || description.contains("围墙倒塌")) {
                    disasterType[7] += 1;
                } else {
                    String address = (String) disaster.get("Disaster_Adrress");
                    if (address.contains("弄") || address.contains("苑") || address.contains("村") || address.contains("城") || address.contains("小区")) {
                        disasterType[8] += 1;
                    } else {
                        disasterType[9] += 1;
                    }
                }
            }
        }

        JSONArray disasterTypeList = new JSONArray();

        for (int i = 0; i < disasterType.length; i++) {
            JSONObject obj = new JSONObject();
            obj.put("type", disasterTypeName[i]);
            obj.put("value", disasterType[i]);
            disasterTypeList.add(obj);
        }

        return disasterTypeList;
    }
}
