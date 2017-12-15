package com.adc.disasterforecast.tools;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

/**
 * ${DESCRIPTION}
 *
 * @author lilin
 * @create 2017-11-16 22:28
 **/
public class WarningHelper {

    public static final String TYPE_WIND = "大风";
    public static final String TYPE_RAIN = "暴雨";
    public static final String TYPE_THUNDER = "雷电";
    public static final String LEVEL_BLUE = "蓝色";
    public static final String LEVEL_YELLOW = "黄色";
    public static final String LEVEL_ORANGE = "橙色";
    public static final String LEVEL_RED = "红色";
    private static String[] warningType = {"rain", "wind", "thunder"};

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
        } else if ("取消".equals(level)) {
            return "cancel";
        } else {
            return "";
        }
    }

    /**
    * @Description 获取台风菲特时间段预警信息，静态
    * @Author lilin
    * @Create 2017/11/24 15:02
    **/
    public static JSONObject getWarningContent() {
        String[] warning1 = {"2013-10-06T20:36:00", "发布", "1", "蓝色", "", "暴雨", "",
                "受“菲特”外围影响，预计6日夜里到7日白天本市累积降水量将达50毫米以上，并可能出现1小时20-40毫米的短时强降水，请注意防范。", ""};
        String[] warning2 = {"2013-10-07T21:52:00", "发布", "1", "黄色", "", "暴雨", "",
                "受“菲特”残余云系和冷空气影响，预计今天夜里到明天下午本市大部地区有6小时50毫米的强降水，本市暴雨蓝色预警信号更新为暴雨黄色预警信号。", ""};
        String[] warning3 = {"2013-10-08T05:36:00", "发布", "1", "橙色", "", "暴雨", "",
                "受热带低压残余和扩散冷空气的共同影响，今天白天仍将维持强降水，3小时累积降水量可达50毫米以上，本市暴雨黄色预警信号更新为暴雨橙色预警信号。", ""};
        String[] warning4 = {"2013-10-08T07:38:00", "发布", "1", "红色", "", "暴雨", "",
                "受热带低压残余和扩散冷空气影响，预计未来3小时内中心城区、松江、闵行、崇明和青浦等地累积降水量将达100毫米左右，本市暴雨橙色预警信号更新为暴雨红色预警信号。", ""};
        String[] warning5 = {"2013-10-08T07:59:00", "发布", "1", "黄色", "", "大风", "",
                "受热带低压残余和扩散冷空气影响，预计今天白天本市将出现阵风7-9级的偏北大风，请注意防范。", ""};
        String[] warning6 = {"2013-10-08T10:00:00", "发布", "1", "黄色", "", "暴雨", "",
                "目前本市大部分地区降水明显减弱，暴雨红色预警信号更新为暴雨黄色预警信号，但浦东东部和南部、奉贤等地仍有强降水，请加强防范。", ""};
        String[] warning7 = {"2013-10-08T11:50:00", "发布", "1", "蓝色", "", "大风", "",
                "本市大风黄色预警信号更新为大风蓝色预警信号。", ""};
        String[] warning8 = {"2013-10-08T11:50:00", "撤销", "", "取消", "", "暴雨", "", "", ""};
        // String[] warning9 = {"2013-10-08T15:45:00", "撤销", "", "取消", "", "大风", "", "", ""};
        List<String[]> warnings = new ArrayList<>(Arrays.asList(warning1, warning2, warning3, warning4, warning5,
                warning6, warning7));
        JSONObject result = new JSONObject();
        JSONArray datas = new JSONArray();
        for (int i = 0; i < 7; i++) {
            JSONObject data = new JSONObject();
            data.put("FORECASTDATE", warnings.get(i)[0]);
            data.put("OPERATION", warnings.get(i)[1]);
            data.put("OPERATION_CODE", warnings.get(i)[2]);
            data.put("LEVEL", warnings.get(i)[3]);
            data.put("LEVEL_CODE", warnings.get(i)[4]);
            data.put("TYPE", warnings.get(i)[5]);
            data.put("TYPE_CODE", warnings.get(i)[6]);
            data.put("CONTENT", warnings.get(i)[7]);
            data.put("GUIDE", warnings.get(i)[8]);
            datas.add(data);
        }
        result.put("Data", datas);
        return result;
    }

    /**
    * @Description 获取杨浦区页面预警服务过程数据 静态
    * @Author lilin
    * @Create 2017/11/28 17:31
    **/
    public static JSONObject getWarningServiceContent() {
        String[] warning1 = {"warning1", "20150616162000", "暴雨积涝风险IV级预警"};
        String[] warning2 = {"warning2", "20150616235300", "暴雨积涝风险III级预警"};
        String[] warning3 = {"warning3", "20150617092900", "解除预警"};
        List<String[]> warnings = new ArrayList<>(Arrays.asList(warning1, warning2, warning3));
        JSONObject result = new JSONObject();
        JSONArray dataArray = new JSONArray();
        for (int i = 0; i < 3; i++) {
            JSONObject data = new JSONObject();
            data.put("id", warnings.get(i)[0]);
            data.put("time", warnings.get(i)[1]);
            data.put("desc", warnings.get(i)[2]);
            dataArray.add(data);
        }
        result.put("Data", dataArray);
        return result;
    }

    public static Map<String, Integer> getWarningMap() {
        Map<String, Integer> warningMap = new HashMap<>();
        for (int i = 0; i < warningType.length; i++) {
            warningMap.put(warningType[i], 0);
        }
        return warningMap;
    }

    public static Map<String, String> getEarlyWarningMap() {
        Map<String, String> warningMap = new HashMap<>();
        for (int i = 0; i < warningType.length; i++) {
            warningMap.put(warningType[i], "normal");
        }
        return warningMap;
    }

    public static int getWarningInInt(String level) {
        if ("蓝色".equals(level)) {
            return 4;
        } else if ("黄色".equals(level)) {
            return 3;
        } else if ("橙色".equals(level)) {
            return 2;
        } else if ("红色".equals(level)) {
            return 1;
        } else {
            return 5;
        }
    }

    public static String getWarningInColor(String level) {
        if ("蓝色".equals(level)) {
            return "blue";
        } else if ("黄色".equals(level)) {
            return "yellow";
        } else if ("橙色".equals(level)) {
            return "orange";
        } else if ("红色".equals(level)) {
            return "red";
        } else {
            return "";
        }
    }
}
