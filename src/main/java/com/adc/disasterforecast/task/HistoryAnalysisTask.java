package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.HistoryAnalysisDataDAO;
import com.adc.disasterforecast.entity.HistoryAnalysisDataEntity;
import com.adc.disasterforecast.global.HistoryAnalysisTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description 历史数据分析
 * @Author lilin
 * @Create 2017-12-07 21:39
 **/
@Component
public class HistoryAnalysisTask {
    // logger for RealTimeControlTask
    private static final Logger logger = LoggerFactory.getLogger(HistoryAnalysisTask.class);

    // dao Autowired
    @Autowired
    private HistoryAnalysisDataDAO historyAnalysisDataDAO;

    private String getLastYearDate() {
        Calendar date = Calendar.getInstance();
        int year = date.get(Calendar.YEAR);
        String lastYearDate = year - 1 + "1231235959";
        return lastYearDate;
    }

    private String getLast10YearDate() {
        Calendar date = Calendar.getInstance();
        int year = date.get(Calendar.YEAR);
        String last10YearDate = year - 10 + "0101000000";
        return last10YearDate;
    }

    /**
    * @Description 近10年天气预警发布情况
    * @Author lilin
    * @Create 2017/12/7 22:10
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
//    @Scheduled(cron = "*/5 * * * * ?")
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsWarnings() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_WARNING_YEAR));

            String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/" + getLast10YearDate() + "/" +
                    getLastYearDate();
            JSONObject obj = HttpHelper.getDataByURL(url);

            JSONArray resultArray = new JSONArray();
            JSONObject resultObject = new JSONObject();
            JSONObject amountObject = new JSONObject();
            JSONObject levelObject = new JSONObject();

            int windNum = 0;
            int rainNum = 0;
            int thunderNum = 0;
            int blueNum = 0;
            int yellowNum = 0;
            int orangeNum = 0;
            int redNum = 0;

            JSONArray warnings = (JSONArray) obj.get("Data");
            for (int i = 0; i < warnings.size(); i++) {
                JSONObject warning = (JSONObject) warnings.get(i);
                if (WarningHelper.TYPE_WIND.equals(warning.get("TYPE"))) {
                    windNum ++;
                }
                if (WarningHelper.TYPE_RAIN.equals(warning.get("TYPE"))) {
                    rainNum ++;
                }
                if (WarningHelper.TYPE_THUNDER.equals(warning.get("TYPE"))) {
                    thunderNum ++;
                }
                if (WarningHelper.LEVEL_BLUE.equals(warning.get("LEVEL"))) {
                    blueNum ++;
                }
                if (WarningHelper.LEVEL_YELLOW.equals(warning.get("LEVEL"))) {
                    yellowNum ++;
                }
                if (WarningHelper.LEVEL_ORANGE.equals(warning.get("LEVEL"))) {
                    orangeNum ++;
                }
                if (WarningHelper.LEVEL_RED.equals(warning.get("LEVEL"))) {
                    redNum ++;
                }
            }

            HistoryAnalysisDataEntity weatherWarningDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                    ("WEATHER_WARNING_INFO");
            JSONArray weatherWarningArray = weatherWarningDataEntity.getValue();
            int size = weatherWarningArray.size();
            for (int i = 0; i < size; i++) {
                Map<String, String> weatherWarningObject = (Map<String, String>) weatherWarningArray.get(i);
                if (WarningHelper.TYPE_WIND.equals(weatherWarningObject.get("TYPE"))) {
                    windNum ++;
                }
                if (WarningHelper.TYPE_RAIN.equals(weatherWarningObject.get("TYPE"))) {
                    rainNum ++;
                }
                if (WarningHelper.TYPE_THUNDER.equals(weatherWarningObject.get("TYPE"))) {
                    thunderNum ++;
                }
                if (WarningHelper.LEVEL_BLUE.equals(weatherWarningObject.get("LEVEL"))) {
                    blueNum ++;
                }
                if (WarningHelper.LEVEL_YELLOW.equals(weatherWarningObject.get("LEVEL"))) {
                    yellowNum ++;
                }
                if (WarningHelper.LEVEL_ORANGE.equals(weatherWarningObject.get("LEVEL"))) {
                    orangeNum ++;
                }
                if (WarningHelper.LEVEL_RED.equals(weatherWarningObject.get("LEVEL"))) {
                    redNum ++;
                }
            }

            amountObject.put("total", warnings.size() + weatherWarningArray.size());
            amountObject.put("wind", windNum);
            amountObject.put("rain", rainNum);
            amountObject.put("thunder", thunderNum);

            levelObject.put("blue", blueNum);
            levelObject.put("yellow", yellowNum);
            levelObject.put("orange", orangeNum);
            levelObject.put("red", redNum);

            resultObject.put("amount", amountObject);
            resultObject.put("level", levelObject);
            resultArray.add(resultObject);

            HistoryAnalysisDataEntity recent10YearsWarnings = new HistoryAnalysisDataEntity();
            recent10YearsWarnings.setName(HistoryAnalysisTaskName.LSSJ_WARNING_YEAR);
            recent10YearsWarnings.setValue(resultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsWarnings);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
    * @Description 获取最近10年的年份
    * @Author lilin
    * @Create 2017/12/7 23:19
    **/
    private int[] getRecent10Years() {
        int[] result = new int[10];
        Calendar date = Calendar.getInstance();
        int year = date.get(Calendar.YEAR);
        for (int i = 0; i < 10; i++) {
            result[i] = year + i - 10;
        }
        return result;
    }

    /**
    * @Description 统计近10年天气预警发布趋势
    * @Author lilin
    * @Create 2017/12/7 23:05
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    //@Scheduled(cron = "*/5 * * * * ?")
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsWarningTrend() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_WARNING_TREND_YEAR));

            String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/" + getLast10YearDate() + "/" +
                    getLastYearDate();
            JSONObject obj = HttpHelper.getDataByURL(url);

            JSONArray resultArray = new JSONArray();
            JSONObject resultObject = new JSONObject();

            JSONObject yearObject = new JSONObject();
            JSONObject monthObject = new JSONObject();
            JSONArray yearRainArray = new JSONArray();
            JSONArray yearWindArray = new JSONArray();
            JSONArray yearThunderArray = new JSONArray();
            JSONArray monthRainArray = new JSONArray();
            JSONArray monthWindArray = new JSONArray();
            JSONArray monthThunderArray = new JSONArray();

            int[] recent10Years = getRecent10Years();
            Map<Integer, Integer> yearRainMap = new HashMap<Integer, Integer>();
            Map<Integer, Integer> yearWindMap = new HashMap<Integer, Integer>();
            Map<Integer, Integer> yearThunderMap = new HashMap<Integer, Integer>();
            for (int i = 0; i < 10; i++) {
                yearRainMap.put(recent10Years[i], 0);
            }
            for (int i = 0; i < 10; i++) {
                yearWindMap.put(recent10Years[i], 0);
            }
            for (int i = 0; i < 10; i++) {
                yearThunderMap.put(recent10Years[i], 0);
            }
            Map<Integer, Integer> monthRainMap = new HashMap<Integer, Integer>();
            Map<Integer, Integer> monthWindMap = new HashMap<Integer, Integer>();
            Map<Integer, Integer> monthThunderMap = new HashMap<Integer, Integer>();
            for (int i = 0; i < 12; i++) {
                monthRainMap.put(i + 1, 0);
            }
            for (int i = 0; i < 12; i++) {
                monthWindMap.put(i + 1, 0);
            }
            for (int i = 0; i < 12; i++) {
                monthThunderMap.put(i + 1, 0);
            }

            JSONArray warnings = (JSONArray) obj.get("Data");
            int size = warnings.size();
            for (int i = 0; i < size; i++) {
                JSONObject warning = (JSONObject) warnings.get(i);
                int year = Integer.parseInt(DateHelper.getYear((String) warning.get("FORECASTDATE")));
                int month = Integer.parseInt(DateHelper.getMonth((String) warning.get("FORECASTDATE")));
                if (WarningHelper.TYPE_WIND.equals(warning.get("TYPE"))) {
                    int yearWindNum = yearWindMap.get(year);
                    yearWindNum ++;
                    yearWindMap.put(year, yearWindNum);

                    int monthWindNum = monthWindMap.get(month);
                    monthWindNum ++;
                    monthWindMap.put(month, monthWindNum);
                }
                if (WarningHelper.TYPE_RAIN.equals(warning.get("TYPE"))) {
                    int yearRainNum = yearRainMap.get(year);
                    yearRainNum ++;
                    yearRainMap.put(year, yearRainNum);

                    int monthRainNum = monthRainMap.get(month);
                    monthRainNum ++;
                    monthRainMap.put(month, monthRainNum);
                }
                if (WarningHelper.TYPE_THUNDER.equals(warning.get("TYPE"))) {
                    int yearThunderNum = yearThunderMap.get(year);
                    yearThunderNum ++;
                    yearThunderMap.put(year, yearThunderNum);

                    int monthThunderNum = monthThunderMap.get(month);
                    monthThunderNum ++;
                    monthThunderMap.put(month, monthThunderNum);
                }
            }

            HistoryAnalysisDataEntity weatherWarningDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                    ("WEATHER_WARNING_INFO");
            JSONArray weatherWarningArray = weatherWarningDataEntity.getValue();
            int weatherWarningArraySize = weatherWarningArray.size();
            for (int i = 0; i < weatherWarningArraySize; i++) {
                Map<String, String> weatherWarningObject = (Map<String, String>) weatherWarningArray.get(i);
                int year = Integer.parseInt(DateHelper.getYear((String) weatherWarningObject.get("FORECASTDATE")));
                int month = Integer.parseInt(DateHelper.getMonth((String) weatherWarningObject.get("FORECASTDATE")));
                if (WarningHelper.TYPE_WIND.equals(weatherWarningObject.get("TYPE"))) {
                    int yearWindNum = yearWindMap.get(year);
                    yearWindNum ++;
                    yearWindMap.put(year, yearWindNum);

                    int monthWindNum = monthWindMap.get(month);
                    monthWindNum ++;
                    monthWindMap.put(month, monthWindNum);
                }
                if (WarningHelper.TYPE_RAIN.equals(weatherWarningObject.get("TYPE"))) {
                    int yearRainNum = yearRainMap.get(year);
                    yearRainNum ++;
                    yearRainMap.put(year, yearRainNum);

                    int monthRainNum = monthRainMap.get(month);
                    monthRainNum ++;
                    monthRainMap.put(month, monthRainNum);
                }
                if (WarningHelper.TYPE_THUNDER.equals(weatherWarningObject.get("TYPE"))) {
                    int yearThunderNum = yearThunderMap.get(year);
                    yearThunderNum ++;
                    yearThunderMap.put(year, yearThunderNum);

                    int monthThunderNum = monthThunderMap.get(month);
                    monthThunderNum ++;
                    monthThunderMap.put(month, monthThunderNum);
                }
            }

            for (int i = 0; i < 10; i++) {
                JSONObject yearRainObject = new JSONObject();
                yearRainObject.put("date", recent10Years[i]);
                yearRainObject.put("value", yearRainMap.get(recent10Years[i]));
                yearRainArray.add(yearRainObject);
                JSONObject yearWindObject = new JSONObject();
                yearWindObject.put("date", recent10Years[i]);
                yearWindObject.put("value", yearWindMap.get(recent10Years[i]));
                yearWindArray.add(yearWindObject);
                JSONObject yearThunderObject = new JSONObject();
                yearThunderObject.put("date", recent10Years[i]);
                yearThunderObject.put("value", yearThunderMap.get(recent10Years[i]));
                yearThunderArray.add(yearThunderObject);
            }

            for (int i = 0; i < 12; i++) {
                JSONObject monthRainObject = new JSONObject();
                monthRainObject.put("date", i + 1);
                monthRainObject.put("value", monthRainMap.get(i + 1));
                monthRainArray.add(monthRainObject);
                JSONObject monthWindObject = new JSONObject();
                monthWindObject.put("date", i + 1);
                monthWindObject.put("value", monthWindMap.get(i + 1));
                monthWindArray.add(monthWindObject);
                JSONObject monthThunderObject = new JSONObject();
                monthThunderObject.put("date", i + 1);
                monthThunderObject.put("value", monthThunderMap.get(i + 1));
                monthThunderArray.add(monthThunderObject);
            }

            yearObject.put("rain", yearRainArray);
            yearObject.put("wind", yearWindArray);
            yearObject.put("thunder", yearThunderArray);

            monthObject.put("rain", monthRainArray);
            monthObject.put("wind", monthWindArray);
            monthObject.put("thunder", monthThunderArray);

            resultObject.put("year", yearObject);
            resultObject.put("month", monthObject);
            resultArray.add(resultObject);

            HistoryAnalysisDataEntity recent10YearsWarningTrend = new HistoryAnalysisDataEntity();
            recent10YearsWarningTrend.setName(HistoryAnalysisTaskName.LSSJ_WARNING_TREND_YEAR);
            recent10YearsWarningTrend.setValue(resultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsWarningTrend);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private String getYearStartDate(int year) {
        String yearStartDate = year + "0101000000";
        return yearStartDate;
    }

    private String getYearEndDate(int year) {
        String yearEndDate = year + "1231235959";
        return yearEndDate;
    }

    /**
    * @Description 获取年度暴雨日、大风日、雷暴日数据
    * @Author lilin
    * @Create 2017/12/8 23:36
    **/
    private int[] getYearWeatherNum(String year, String code) {
        HistoryAnalysisDataEntity weatherDisasterDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                ("WEATHER_DISASTER_INFO");
        JSONArray weatherDisasterArray = weatherDisasterDataEntity.getValue();
        int size = weatherDisasterArray.size();
        int[] num = new int[3];
        for (int i = 0; i < size; i++) {
            Map<String, String> weatherDisasterObject = (Map<String, String>) weatherDisasterArray.get(i);
            if (code.equals(weatherDisasterObject.get("区站号"))) {
                if (year.equals(weatherDisasterObject.get("年"))) {
                    int rainNum = Integer.parseInt((String) weatherDisasterObject.get("暴雨日数"));
                    int windNum = Integer.parseInt((String) weatherDisasterObject.get("大风日数"));
                    int thunderNum = Integer.parseInt((String) weatherDisasterObject.get("雷暴日数"));
                    num[0] += rainNum;
                    num[1] += windNum;
                    num[2] += thunderNum;
                }
            }
        }
        return num;
    }

    /**
    * @Description 获取近10年的年度暴雨日、大风日、雷暴日数据
    * @Author lilin
    * @Create 2017/12/9 10:06
    **/
    private Map<Integer, int[]> getRecent10YearsWeatherNum(int[] years, String code) {
        HistoryAnalysisDataEntity weatherDisasterDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                ("WEATHER_DISASTER_INFO");
        JSONArray weatherDisasterArray = weatherDisasterDataEntity.getValue();
        int size = weatherDisasterArray.size();

        Map<Integer, int[]> result = new HashMap<Integer, int[]>();
        for (int i = 0; i < 10; i++) {
            int[] num = new int[3];
            for (int j = 0; j < size; j++) {
                Map<String, String> weatherDisasterObject = (Map<String, String>) weatherDisasterArray.get(j);
                if (code.equals(weatherDisasterObject.get("区站号"))) {
                    if ((years[i] + "").equals(weatherDisasterObject.get("年"))) {
                        int rainNum = Integer.parseInt((String) weatherDisasterObject.get("暴雨日数"));
                        int windNum = Integer.parseInt((String) weatherDisasterObject.get("大风日数"));
                        int thunderNum = Integer.parseInt((String) weatherDisasterObject.get("雷暴日数"));
                        num[0] += rainNum;
                        num[1] += windNum;
                        num[2] += thunderNum;
                    }
                }
            }
            result.put(years[i], num);
        }
        return result;
    }

    /**
    * @Description 获取月份暴雨日、大风日、雷暴日数据
    * @Author lilin
    * @Create 2017/12/8 23:57
    **/
    private int[] getMonthWeatherNum(String month, String code, int[] years) {
        HistoryAnalysisDataEntity weatherDisasterDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                ("WEATHER_DISASTER_INFO");
        JSONArray weatherDisasterArray = weatherDisasterDataEntity.getValue();
        int size = weatherDisasterArray.size();
        int[] num = new int[3];
        for (int i = 0; i < size; i++) {
            Map<String, String> weatherDisasterObject = (Map<String, String>) weatherDisasterArray.get(i);
            if (code.equals(weatherDisasterObject.get("区站号"))) {
                int year = Integer.parseInt((String) weatherDisasterObject.get("年"));
                if (month.equals(weatherDisasterObject.get("月")) && year >= years[0] && year <= years[9]) {
                    int rainNum = Integer.parseInt((String) weatherDisasterObject.get("暴雨日数"));
                    int windNum = Integer.parseInt((String) weatherDisasterObject.get("大风日数"));
                    int thunderNum = Integer.parseInt((String) weatherDisasterObject.get("雷暴日数"));
                    num[0] += rainNum;
                    num[1] += windNum;
                    num[2] += thunderNum;
                }
            }
        }
        return num;
    }

    /**
    * @Description 获取近10年的月份暴雨日、大风日、雷暴日数据
    * @Author lilin
    * @Create 2017/12/9 10:13
    **/
    private Map<Integer, int[]> getRecent10YearsMonthWeatherNum(int[] years, String code) {
        HistoryAnalysisDataEntity weatherDisasterDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                ("WEATHER_DISASTER_INFO");
        JSONArray weatherDisasterArray = weatherDisasterDataEntity.getValue();
        int size = weatherDisasterArray.size();

        Map<Integer, int[]> result = new HashMap<Integer, int[]>();
        for (int i = 1; i < 13; i++) {
            int[] num = new int[3];
            for (int j = 0; j < size; j++) {
                Map<String, String> weatherDisasterObject = (Map<String, String>) weatherDisasterArray.get(j);
                if (code.equals(weatherDisasterObject.get("区站号"))) {
                    int year = Integer.parseInt((String) weatherDisasterObject.get("年"));
                    if ((i + "").equals(weatherDisasterObject.get("月")) && year >= years[0] && year <= years[9]) {
                        int rainNum = Integer.parseInt((String) weatherDisasterObject.get("暴雨日数"));
                        int windNum = Integer.parseInt((String) weatherDisasterObject.get("大风日数"));
                        int thunderNum = Integer.parseInt((String) weatherDisasterObject.get("雷暴日数"));
                        num[0] += rainNum;
                        num[1] += windNum;
                        num[2] += thunderNum;
                    }
                }
            }
            result.put(i, num);
        }
        return result;
    }

    /**
    * @Description 统计近10年气象事件造成的灾害（徐汇区）
    * @Author lilin
    * @Create 2017/12/8 22:47
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    //@Scheduled(cron = "*/5 * * * * ?")
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsWeatherDisaster() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_WEATHER_DISASTER));

            int[] recent10Years = getRecent10Years();

            int totalRainNum = 0;
            int totalWindNum = 0;
            int totalThunderNum = 0;

            Map<Integer, int[]> recent10YearsWeatherNum = getRecent10YearsWeatherNum(recent10Years, AutoStationHelper
                    .AREA_STATION_CODE_XUJIAHUI);
            Map<Integer, JSONObject> yearRainWeatherDisasterMap = new HashMap<Integer, JSONObject>();
            Map<Integer, JSONObject> yearWindWeatherDisasterMap = new HashMap<Integer, JSONObject>();
            Map<Integer, JSONObject> yearThunderWeatherDisasterMap = new HashMap<Integer, JSONObject>();
            for (int i = 0; i < 10; i++) {
                JSONObject yearRainWeatherDisasterObject = new JSONObject();
//            int[] yearWeatherNum = getYearWeatherNum(recent10Years[i] + "", AutoStationHelper.AREA_STATION_CODE_XUJIAHUI);
//            yearRainWeatherDisasterObject.put("weatherValue", yearWeatherNum[0]);
//            yearRainWeatherDisasterObject.put("disasterValue", 0);
//            yearRainWeatherDisasterMap.put(recent10Years[i], yearRainWeatherDisasterObject);
//            JSONObject yearWindWeatherDisasterObject = new JSONObject();
//            yearWindWeatherDisasterObject.put("weatherValue", yearWeatherNum[1]);
//            yearWindWeatherDisasterObject.put("disasterValue", 0);
//            yearWindWeatherDisasterMap.put(recent10Years[i], yearWindWeatherDisasterObject);
//            JSONObject yearThunderWeatherDisasterObject = new JSONObject();
//            yearThunderWeatherDisasterObject.put("weatherValue", yearWeatherNum[2]);
//            yearThunderWeatherDisasterObject.put("disasterValue", 0);
//            yearThunderWeatherDisasterMap.put(recent10Years[i], yearThunderWeatherDisasterObject);

                yearRainWeatherDisasterObject.put("weatherValue", recent10YearsWeatherNum.get(recent10Years[i])[0]);
                yearRainWeatherDisasterObject.put("disasterValue", 0);
                yearRainWeatherDisasterMap.put(recent10Years[i], yearRainWeatherDisasterObject);
                JSONObject yearWindWeatherDisasterObject = new JSONObject();
                yearWindWeatherDisasterObject.put("weatherValue", recent10YearsWeatherNum.get(recent10Years[i])[1]);
                yearWindWeatherDisasterObject.put("disasterValue", 0);
                yearWindWeatherDisasterMap.put(recent10Years[i], yearWindWeatherDisasterObject);
                JSONObject yearThunderWeatherDisasterObject = new JSONObject();
                yearThunderWeatherDisasterObject.put("weatherValue", recent10YearsWeatherNum.get(recent10Years[i])[2]);
                yearThunderWeatherDisasterObject.put("disasterValue", 0);
                yearThunderWeatherDisasterMap.put(recent10Years[i], yearThunderWeatherDisasterObject);
            }

            Map<Integer, int[]> recent10YearsMonthWeatherNum = getRecent10YearsMonthWeatherNum(recent10Years, AutoStationHelper
                    .AREA_STATION_CODE_XUJIAHUI);
            Map<Integer, JSONObject> monthRainWeatherDisasterMap = new HashMap<Integer, JSONObject>();
            Map<Integer, JSONObject> monthWindWeatherDisasterMap = new HashMap<Integer, JSONObject>();
            Map<Integer, JSONObject> monthThunderWeatherDisasterMap = new HashMap<Integer, JSONObject>();
            for (int i = 1; i < 13; i++) {
                JSONObject monthRainWeatherDisasterObject = new JSONObject();
//            int[] monthWeatherNum = getMonthWeatherNum(i + "", AutoStationHelper.AREA_STATION_CODE_XUJIAHUI,
//                    recent10Years);
//            monthRainWeatherDisasterObject.put("weatherValue", monthWeatherNum[0]);
//            monthRainWeatherDisasterObject.put("disasterValue", 0);
//            monthRainWeatherDisasterMap.put(i, monthRainWeatherDisasterObject);
//            JSONObject monthWindWeatherDisasterObject = new JSONObject();
//            monthWindWeatherDisasterObject.put("weatherValue", monthWeatherNum[1]);
//            monthWindWeatherDisasterObject.put("disasterValue", 0);
//            monthWindWeatherDisasterMap.put(i, monthWindWeatherDisasterObject);
//            JSONObject monthThunderWeatherDisasterObject = new JSONObject();
//            monthThunderWeatherDisasterObject.put("weatherValue", monthWeatherNum[2]);
//            monthThunderWeatherDisasterObject.put("disasterValue", 0);
//            monthThunderWeatherDisasterMap.put(i, monthThunderWeatherDisasterObject);

                monthRainWeatherDisasterObject.put("weatherValue", recent10YearsMonthWeatherNum.get(i)[0]);
                monthRainWeatherDisasterObject.put("disasterValue", 0);
                monthRainWeatherDisasterMap.put(i, monthRainWeatherDisasterObject);
                JSONObject monthWindWeatherDisasterObject = new JSONObject();
                monthWindWeatherDisasterObject.put("weatherValue", recent10YearsMonthWeatherNum.get(i)[1]);
                monthWindWeatherDisasterObject.put("disasterValue", 0);
                monthWindWeatherDisasterMap.put(i, monthWindWeatherDisasterObject);
                JSONObject monthThunderWeatherDisasterObject = new JSONObject();
                monthThunderWeatherDisasterObject.put("weatherValue", recent10YearsMonthWeatherNum.get(i)[2]);
                monthThunderWeatherDisasterObject.put("disasterValue", 0);
                monthThunderWeatherDisasterMap.put(i, monthThunderWeatherDisasterObject);
            }

            for (int i = 0; i < 10; i++) {
                String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterDetailData_Geliku/" + getYearStartDate
                        (recent10Years[i]) + "/" + getYearEndDate(recent10Years[i]);
                JSONObject obj = HttpHelper.getDataByURL(url);
                JSONArray disasters = (JSONArray) obj.get("Data");
                int size = disasters.size();
                int yearRainDisasterNum = 0;
                int yearWindDisasterNum = 0;
                int yearThunderDisasterNum = 0;

                for (int j = 0; j < size; j++) {
                    JSONObject disaster = (JSONObject) disasters.get(j);
                    int month = Integer.parseInt(DateHelper.getMonth((String) disaster.get("DATETIME_DISASTER")));
                    if (DisasterTypeHelper.DISTRICT_CODE == (long) disaster.get("DISTRICT")) {
                        if (DisasterTypeHelper.DISASTER_RAIN_CODE == (long) disaster.get("CODE_DISASTER")) {
                            totalRainNum ++;
                            yearRainDisasterNum ++;

                            JSONObject monthRainWeatherDisasterObject = monthRainWeatherDisasterMap.get(month);
                            int disasterValue = (int) monthRainWeatherDisasterObject.get("disasterValue");
                            disasterValue ++;
                            monthRainWeatherDisasterObject.put("disasterValue", disasterValue);
                            monthRainWeatherDisasterMap.put(month, monthRainWeatherDisasterObject);
                        }
                        if (DisasterTypeHelper.DISASTER_WIND_CODE == (long) disaster.get("CODE_DISASTER")) {
                            totalWindNum ++;
                            yearWindDisasterNum ++;

                            JSONObject monthWindWeatherDisasterObject = monthWindWeatherDisasterMap.get(month);
                            int disasterValue = (int) monthWindWeatherDisasterObject.get("disasterValue");
                            disasterValue ++;
                            monthWindWeatherDisasterObject.put("disasterValue", disasterValue);
                            monthWindWeatherDisasterMap.put(month, monthWindWeatherDisasterObject);
                        }
                        if (DisasterTypeHelper.DISASTER_THUNDER_CODE == (long) disaster.get("CODE_DISASTER")) {
                            totalThunderNum ++;
                            yearThunderDisasterNum ++;

                            JSONObject monthThunderWeatherDisasterObject = monthThunderWeatherDisasterMap.get(month);
                            int disasterValue = (int) monthThunderWeatherDisasterObject.get("disasterValue");
                            disasterValue ++;
                            monthThunderWeatherDisasterObject.put("disasterValue", disasterValue);
                            monthThunderWeatherDisasterMap.put(month, monthThunderWeatherDisasterObject);
                        }
                    }
                }
                JSONObject yearRainWeatherDisasterObject = yearRainWeatherDisasterMap.get(recent10Years[i]);
                yearRainWeatherDisasterObject.put("disasterValue", yearRainDisasterNum);
                yearRainWeatherDisasterMap.put(recent10Years[i], yearRainWeatherDisasterObject);
                JSONObject yearWindWeatherDisasterObject = yearWindWeatherDisasterMap.get(recent10Years[i]);
                yearWindWeatherDisasterObject.put("disasterValue", yearWindDisasterNum);
                yearWindWeatherDisasterMap.put(recent10Years[i], yearWindWeatherDisasterObject);
                JSONObject yearThunderWeatherDisasterObject = yearThunderWeatherDisasterMap.get(recent10Years[i]);
                yearThunderWeatherDisasterObject.put("disasterValue", yearThunderDisasterNum);
                yearThunderWeatherDisasterMap.put(recent10Years[i], yearThunderWeatherDisasterObject);
            }

            JSONArray resultArray = new JSONArray();
            JSONObject resultObject = new JSONObject();
            JSONObject totalResultObject = new JSONObject();
            JSONObject yearResultObject = new JSONObject();
            JSONObject monthResultObject = new JSONObject();
            JSONArray yearRainResultArray = new JSONArray();
            JSONArray yearWindResultArray = new JSONArray();
            JSONArray yearThunderResultArray = new JSONArray();
            JSONArray monthRainResultArray = new JSONArray();
            JSONArray monthWindResultArray = new JSONArray();
            JSONArray monthThunderResultArray = new JSONArray();

            totalResultObject.put("rain", totalRainNum);
            totalResultObject.put("wind", totalWindNum);
            totalResultObject.put("thunder", totalThunderNum);

            for (int index = 0; index < 10; index++) {
                int key = recent10Years[index];
                JSONObject value = yearRainWeatherDisasterMap.get(key);
                JSONObject yearRainResultObject = new JSONObject();
                yearRainResultObject.put("date", key);
                yearRainResultObject.put("weatherValue", value.get("weatherValue"));
                yearRainResultObject.put("disasterValue", value.get("disasterValue"));
                yearRainResultArray.add(yearRainResultObject);
            }
            for (int index = 0; index < 10; index++) {
                int key = recent10Years[index];
                JSONObject value = yearWindWeatherDisasterMap.get(key);
                JSONObject yearWindResultObject = new JSONObject();
                yearWindResultObject.put("date", key);
                yearWindResultObject.put("weatherValue", value.get("weatherValue"));
                yearWindResultObject.put("disasterValue", value.get("disasterValue"));
                yearWindResultArray.add(yearWindResultObject);
            }
            for (int index = 0; index < 10; index++) {
                int key = recent10Years[index];
                JSONObject value = yearThunderWeatherDisasterMap.get(key);
                JSONObject yearThunderResultObject = new JSONObject();
                yearThunderResultObject.put("date", key);
                yearThunderResultObject.put("weatherValue", value.get("weatherValue"));
                yearThunderResultObject.put("disasterValue", value.get("disasterValue"));
                yearThunderResultArray.add(yearThunderResultObject);
            }
//        for (Map.Entry<Integer, JSONObject> entry : yearRainWeatherDisasterMap.entrySet()) {
//            int key = entry.getKey();
//            JSONObject value = entry.getValue();
//            JSONObject yearRainResultObject = new JSONObject();
//            yearRainResultObject.put("date", key);
//            yearRainResultObject.put("weatherValue", value.get("weatherValue"));
//            yearRainResultObject.put("disasterValue", value.get("disasterValue"));
//            yearRainResultArray.add(yearRainResultObject);
//        }
//        for (Map.Entry<Integer, JSONObject> entry : yearWindWeatherDisasterMap.entrySet()) {
//            int key = entry.getKey();
//            JSONObject value = entry.getValue();
//            JSONObject yearWindResultObject = new JSONObject();
//            yearWindResultObject.put("date", key);
//            yearWindResultObject.put("weatherValue", value.get("weatherValue"));
//            yearWindResultObject.put("disasterValue", value.get("disasterValue"));
//            yearWindResultArray.add(yearWindResultObject);
//        }
//        for (Map.Entry<Integer, JSONObject> entry : yearThunderWeatherDisasterMap.entrySet()) {
//            int key = entry.getKey();
//            JSONObject value = entry.getValue();
//            JSONObject yearThunderResultObject = new JSONObject();
//            yearThunderResultObject.put("date", key);
//            yearThunderResultObject.put("weatherValue", value.get("weatherValue"));
//            yearThunderResultObject.put("disasterValue", value.get("disasterValue"));
//            yearThunderResultArray.add(yearThunderResultObject);
//        }
            yearResultObject.put("rain", yearRainResultArray);
            yearResultObject.put("wind", yearWindResultArray);
            yearResultObject.put("thunder", yearThunderResultArray);

            for (Map.Entry<Integer, JSONObject> entry : monthRainWeatherDisasterMap.entrySet()) {
                int key = entry.getKey();
                JSONObject value = entry.getValue();
                JSONObject monthRainResultObject = new JSONObject();
                monthRainResultObject.put("date", key);
                monthRainResultObject.put("weatherValue", value.get("weatherValue"));
                monthRainResultObject.put("disasterValue", value.get("disasterValue"));
                monthRainResultArray.add(monthRainResultObject);
            }
            for (Map.Entry<Integer, JSONObject> entry : monthWindWeatherDisasterMap.entrySet()) {
                int key = entry.getKey();
                JSONObject value = entry.getValue();
                JSONObject monthWindResultObject = new JSONObject();
                monthWindResultObject.put("date", key);
                monthWindResultObject.put("weatherValue", value.get("weatherValue"));
                monthWindResultObject.put("disasterValue", value.get("disasterValue"));
                monthWindResultArray.add(monthWindResultObject);
            }
            for (Map.Entry<Integer, JSONObject> entry : monthThunderWeatherDisasterMap.entrySet()) {
                int key = entry.getKey();
                JSONObject value = entry.getValue();
                JSONObject monthThunderResultObject = new JSONObject();
                monthThunderResultObject.put("date", key);
                monthThunderResultObject.put("weatherValue", value.get("weatherValue"));
                monthThunderResultObject.put("disasterValue", value.get("disasterValue"));
                monthThunderResultArray.add(monthThunderResultObject);
            }
            monthResultObject.put("rain", monthRainResultArray);
            monthResultObject.put("wind", monthWindResultArray);
            monthResultObject.put("thunder", monthThunderResultArray);

            resultObject.put("total", totalResultObject);
            resultObject.put("year", yearResultObject);
            resultObject.put("month", monthResultObject);
            resultArray.add(resultObject);

            HistoryAnalysisDataEntity recent10YearsWeatherDisaster = new HistoryAnalysisDataEntity();
            recent10YearsWeatherDisaster.setName(HistoryAnalysisTaskName.LSSJ_WEATHER_DISASTER);
            recent10YearsWeatherDisaster.setValue(resultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsWeatherDisaster);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
    * @Description 获取近十年暴雨/大风频次信息
    * @Author lilin
    * @Create 2017/12/9 0:38
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    //@Scheduled(cron = "*/5 * * * * ?")
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsDisasterFrequency() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_FREQUENCY));

            int[] recent10Years = getRecent10Years();

            HistoryAnalysisDataEntity autoStationInfoDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                    ("AUTO_STATION_INFO");
            JSONArray autoStationInfoArray = autoStationInfoDataEntity.getValue();
            HistoryAnalysisDataEntity weatherDisasterDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName
                    ("WEATHER_DISASTER_INFO");
            JSONArray weatherDisasterArray = weatherDisasterDataEntity.getValue();

            int size = weatherDisasterArray.size();

            Map<String, JSONObject> rainDisasterFrequencyMap = new HashMap<String, JSONObject>();
            Map<String, JSONObject> windDisasterFrequencyMap = new HashMap<String, JSONObject>();
            for (int i = 0; i < 11; i++) {
                Map<String, Object> autoStationInfoObject = (Map<String, Object>) autoStationInfoArray.get(i);
                JSONObject rainDisasterFrequencyObject = new JSONObject();
                rainDisasterFrequencyObject.put("lon", (double) autoStationInfoObject.get("lon"));
                rainDisasterFrequencyObject.put("lat", (double) autoStationInfoObject.get("lat"));
                rainDisasterFrequencyObject.put("value", 0);
                rainDisasterFrequencyMap.put((String) autoStationInfoObject.get("stationCode"),
                        rainDisasterFrequencyObject);
                JSONObject windDisasterFrequencyObject = new JSONObject();
                windDisasterFrequencyObject.put("lon", (double) autoStationInfoObject.get("lon"));
                windDisasterFrequencyObject.put("lat", (double) autoStationInfoObject.get("lat"));
                windDisasterFrequencyObject.put("value", 0);
                windDisasterFrequencyMap.put((String) autoStationInfoObject.get("stationCode"),
                        windDisasterFrequencyObject);
            }

            for (int i = 0; i < size; i++) {
                Map<String, String> weatherDisasterObject = (Map<String, String>) weatherDisasterArray.get(i);
                int year = Integer.parseInt((String) weatherDisasterObject.get("年"));
                if (year >= recent10Years[0] && year <= recent10Years[9]){
                    String stationCode = weatherDisasterObject.get("区站号");
                    int rainNum = Integer.parseInt((String) weatherDisasterObject.get("暴雨日数"));
                    int windNum = Integer.parseInt((String) weatherDisasterObject.get("大风日数"));
                    JSONObject rainObject = rainDisasterFrequencyMap.get(stationCode);
                    JSONObject windObject = windDisasterFrequencyMap.get(stationCode);
                    if (rainObject != null) {
                        int rainValue = (int) rainObject.get("value");
                        rainValue += rainNum;
                        rainObject.put("value", rainValue);
                        rainDisasterFrequencyMap.put(stationCode, rainObject);
                    }
                    if (windObject != null) {
                        int windValue = (int) windObject.get("value");
                        windValue += windNum;
                        windObject.put("value", windValue);
                        windDisasterFrequencyMap.put(stationCode, windObject);
                    }
                }
            }

            JSONArray resultArray = new JSONArray();
            JSONObject resultObject = new JSONObject();
            JSONArray rainResultArray = new JSONArray();
            JSONArray windResultArray = new JSONArray();

            for (Map.Entry<String, JSONObject> entry : rainDisasterFrequencyMap.entrySet()) {
                String key = entry.getKey();
                JSONObject value = entry.getValue();
                JSONObject rainDisasterFrequencyResultObject = new JSONObject();
                rainDisasterFrequencyResultObject.put("lon", value.get("lon"));
                rainDisasterFrequencyResultObject.put("lat", value.get("lat"));
                rainDisasterFrequencyResultObject.put("value", value.get("value"));
                rainResultArray.add(rainDisasterFrequencyResultObject);
            }
            for (Map.Entry<String, JSONObject> entry : windDisasterFrequencyMap.entrySet()) {
                String key = entry.getKey();
                JSONObject value = entry.getValue();
                JSONObject windDisasterFrequencyResultObject = new JSONObject();
                windDisasterFrequencyResultObject.put("lon", value.get("lon"));
                windDisasterFrequencyResultObject.put("lat", value.get("lat"));
                windDisasterFrequencyResultObject.put("value", value.get("value"));
                windResultArray.add(windDisasterFrequencyResultObject);
            }
            resultObject.put("rain", rainResultArray);
            resultObject.put("wind", windResultArray);
            resultArray.add(resultObject);

            HistoryAnalysisDataEntity recent10YearsDisasterFrequency = new HistoryAnalysisDataEntity();
            recent10YearsDisasterFrequency.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_FREQUENCY);
            recent10YearsDisasterFrequency.setValue(resultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterFrequency);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
    * @Description 统计近10年灾情密度、灾情年度均值、年分布、月分布、日分布、类型分析
    * @Author lilin
    * @Create 2017/12/8 0:07
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    //@Scheduled(cron = "*/5 * * * * ?")
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsDisaster() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_AVG));
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_YEAR));
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_MONTH));
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_DAY));
            //logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TYPE));
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_DENSITY));

            int[] recent10Years = getRecent10Years();

            Map<Integer, Integer> yearDisasterMap = new HashMap<Integer, Integer>();
            for (int i = 0; i < 10; i++) {
                yearDisasterMap.put(recent10Years[i], 0);
            }

            Map<Integer, Integer> monthDisasterMap = new HashMap<Integer, Integer>();
            for (int i = 0; i < 12; i++) {
                monthDisasterMap.put(i + 1, 0);
            }

            Map<Integer, Integer> dayDisasterMap = new HashMap<Integer, Integer>();
            for (int i = 0; i <= 24; i++) {
                dayDisasterMap.put(i, 0);
            }

            Map<String, Integer> rainDisasterDensityMap = new HashMap<String, Integer>();
            Map<String, Integer> windDisasterDensityMap = new HashMap<String, Integer>();
            Map<String, Integer> thunderDisasterDensityMap = new HashMap<String, Integer>();

            int totalNum = 0;
//        int GKZWNum = 0;
//        int FWJSNum = 0;
//        int NTJSNum = 0;
//        int OthersNum = 0;
            for (int i = 0; i < 10; i++) {
                String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterDetailData_Geliku/" + getYearStartDate
                        (recent10Years[i]) + "/" + getYearEndDate(recent10Years[i]);
                JSONObject obj = HttpHelper.getDataByURL(url);
                JSONArray disasters = (JSONArray) obj.get("Data");
                int size = disasters.size();
                totalNum = totalNum + size;

                yearDisasterMap.put(recent10Years[i], size);

                for (int j = 0; j < size; j++) {
                    JSONObject disaster = (JSONObject) disasters.get(j);
                    int month = Integer.parseInt(DateHelper.getMonth((String) disaster.get("DATETIME_DISASTER")));
                    int hour = Integer.parseInt(DateHelper.getHour((String) disaster.get("DATETIME_DISASTER")));

                    int monthDisasterNum = monthDisasterMap.get(month);
                    monthDisasterNum ++;
                    monthDisasterMap.put(month, monthDisasterNum);

                    int dayDisasterNum = dayDisasterMap.get(hour);
                    dayDisasterNum ++;
                    dayDisasterMap.put(hour, dayDisasterNum);

//                String desc = (String) disaster.get("CASE_ADDR") + (String) disaster.get("CASE_DESC") + (String)
//                        disaster.get("ACCEPTER");
//                if ("高空坠物".equals(DisasterTypeHelper.getDisasterType(desc))) {
//                    GKZWNum ++;
//                }
//                if ("房屋进水".equals(DisasterTypeHelper.getDisasterType(desc))) {
//                    FWJSNum ++;
//                }
//                if ("农田进水".equals(DisasterTypeHelper.getDisasterType(desc))) {
//                    NTJSNum ++;
//                }
//                if ("其他".equals(DisasterTypeHelper.getDisasterType(desc))) {
//                    OthersNum ++;
//                }

                    long disasterCode = (long) disaster.get("CODE_DISASTER");
                    DecimalFormat decimalFormat = new DecimalFormat("#.00");
                    double lontitude = Double.parseDouble(decimalFormat.format((double) disaster.get("LONTITUDE")));
                    double latitude = Double.parseDouble(decimalFormat.format((double) disaster.get("LATITUDE")));
                    String lon_lat_key = lontitude + "-" + latitude;
                    if (DisasterTypeHelper.DISASTER_RAIN_CODE == disasterCode) {
                        int rainDisasterDensityNum = 0;
                        if (rainDisasterDensityMap.get(lon_lat_key) != null &&
                                rainDisasterDensityMap.get(lon_lat_key) >= 0) {
                            rainDisasterDensityNum = rainDisasterDensityMap.get(lon_lat_key);
                        }
                        rainDisasterDensityNum ++;
                        rainDisasterDensityMap.put(lon_lat_key, rainDisasterDensityNum);
                    }
                    if (DisasterTypeHelper.DISASTER_WIND_CODE == disasterCode) {
                        int windDisasterDensityNum = 0;
                        if (windDisasterDensityMap.get(lon_lat_key) != null &&
                                windDisasterDensityMap.get(lon_lat_key) >= 0) {
                            windDisasterDensityNum = windDisasterDensityMap.get(lon_lat_key);
                        }
                        windDisasterDensityNum ++;
                        windDisasterDensityMap.put(lon_lat_key, windDisasterDensityNum);
                    }
                    if (DisasterTypeHelper.DISASTER_THUNDER_CODE == disasterCode) {
                        int thunderDisasterDensityNum = 0;
                        if (thunderDisasterDensityMap.get(lon_lat_key) != null &&
                                thunderDisasterDensityMap.get(lon_lat_key) >= 0) {
                            thunderDisasterDensityNum = thunderDisasterDensityMap.get(lon_lat_key);
                        }
                        thunderDisasterDensityNum ++;
                        thunderDisasterDensityMap.put(lon_lat_key, thunderDisasterDensityNum);
                    }
                }
            }
            int averageNum = totalNum / 10;

            JSONArray disasterAverageResultArray = new JSONArray();
            JSONObject disasterAverageResultObject = new JSONObject();

            disasterAverageResultObject.put("total", averageNum);
            disasterAverageResultArray.add(disasterAverageResultObject);

            HistoryAnalysisDataEntity recent10YearsDisasterAverage = new HistoryAnalysisDataEntity();
            recent10YearsDisasterAverage.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_AVG);
            recent10YearsDisasterAverage.setValue(disasterAverageResultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterAverage);

            JSONArray disasterTrendYearResultArray = new JSONArray();

            for (int i = 0; i < 10; i++) {
                JSONObject disasterTrendYearResultObject = new JSONObject();
                disasterTrendYearResultObject.put("date", recent10Years[i]);
                disasterTrendYearResultObject.put("value", yearDisasterMap.get(recent10Years[i]));
                disasterTrendYearResultArray.add(disasterTrendYearResultObject);
            }

            HistoryAnalysisDataEntity recent10YearsDisasterTrendYear = new HistoryAnalysisDataEntity();
            recent10YearsDisasterTrendYear.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_YEAR);
            recent10YearsDisasterTrendYear.setValue(disasterTrendYearResultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterTrendYear);

            JSONArray disasterTrendMonthResultArray = new JSONArray();

            for (int i = 0; i < 12; i++) {
                JSONObject disasterTrendMonthResultObject = new JSONObject();
                disasterTrendMonthResultObject.put("date", i + 1);
                disasterTrendMonthResultObject.put("value", monthDisasterMap.get(i + 1));
                disasterTrendMonthResultArray.add(disasterTrendMonthResultObject);
            }

            HistoryAnalysisDataEntity recent10YearsDisasterTrendMonth = new HistoryAnalysisDataEntity();
            recent10YearsDisasterTrendMonth.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_MONTH);
            recent10YearsDisasterTrendMonth.setValue(disasterTrendMonthResultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterTrendMonth);

            JSONArray disasterTrendDayResultArray = new JSONArray();

            for (int i = 0; i <= 24; i++) {
                JSONObject disasterTrendDayResultObject = new JSONObject();
                disasterTrendDayResultObject.put("date", i);
                disasterTrendDayResultObject.put("value", dayDisasterMap.get(i));
                disasterTrendDayResultArray.add(disasterTrendDayResultObject);
            }

            HistoryAnalysisDataEntity recent10YearsDisasterTrendDay = new HistoryAnalysisDataEntity();
            recent10YearsDisasterTrendDay.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_DAY);
            recent10YearsDisasterTrendDay.setValue(disasterTrendDayResultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterTrendDay);

//        JSONArray disasterTypeResultArray = new JSONArray();
//        JSONObject GKZWResultObject = new JSONObject();
//        JSONObject FWJSResultObject = new JSONObject();
//        JSONObject NTJSResultObject = new JSONObject();
//        JSONObject OthersResultObject = new JSONObject();
//        GKZWResultObject.put("type", "高空坠物");
//        GKZWResultObject.put("value", GKZWNum);
//        FWJSResultObject.put("type", "房屋进水");
//        FWJSResultObject.put("value", FWJSNum);
//        NTJSResultObject.put("type", "农田进水");
//        NTJSResultObject.put("value", NTJSNum);
//        OthersResultObject.put("type", "其他");
//        OthersResultObject.put("value", OthersNum);
//        disasterTypeResultArray.add(GKZWResultObject);
//        disasterTypeResultArray.add(FWJSResultObject);
//        disasterTypeResultArray.add(NTJSResultObject);
//        disasterTypeResultArray.add(OthersResultObject);

//        HistoryAnalysisDataEntity recent10YearsDisasterType = new HistoryAnalysisDataEntity();
//        recent10YearsDisasterType.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_TYPE);
//        recent10YearsDisasterType.setValue(disasterTypeResultArray);
//
//        historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterType);

            JSONArray disasterDensityResultArray = new JSONArray();
            JSONObject disasterDensityResultObject = new JSONObject();
            JSONArray rainDisasterDensityArray = new JSONArray();
            JSONArray windDisasterDensityArray = new JSONArray();
            JSONArray thunderDisasterDensityArray = new JSONArray();

            for (Map.Entry<String, Integer> entry : rainDisasterDensityMap.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
                String[] keyParts = key.split("-");
                double lon = Double.parseDouble(keyParts[0]);
                double lat = Double.parseDouble(keyParts[1]);
                JSONObject rainDisasterDensityObject = new JSONObject();
                rainDisasterDensityObject.put("lon", lon);
                rainDisasterDensityObject.put("lat", lat);
                rainDisasterDensityObject.put("value", value);
                rainDisasterDensityArray.add(rainDisasterDensityObject);
            }
            for (Map.Entry<String, Integer> entry : windDisasterDensityMap.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
                String[] keyParts = key.split("-");
                double lon = Double.parseDouble(keyParts[0]);
                double lat = Double.parseDouble(keyParts[1]);
                JSONObject windDisasterDensityObject = new JSONObject();
                windDisasterDensityObject.put("lon", lon);
                windDisasterDensityObject.put("lat", lat);
                windDisasterDensityObject.put("value", value);
                windDisasterDensityArray.add(windDisasterDensityObject);
            }
            for (Map.Entry<String, Integer> entry : thunderDisasterDensityMap.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
                String[] keyParts = key.split("-");
                double lon = Double.parseDouble(keyParts[0]);
                double lat = Double.parseDouble(keyParts[1]);
                JSONObject thunderDisasterDensityObject = new JSONObject();
                thunderDisasterDensityObject.put("lon", lon);
                thunderDisasterDensityObject.put("lat", lat);
                thunderDisasterDensityObject.put("value", value);
                thunderDisasterDensityArray.add(thunderDisasterDensityObject);
            }
            disasterDensityResultObject.put("rain", rainDisasterDensityArray);
            disasterDensityResultObject.put("wind", windDisasterDensityArray);
            disasterDensityResultObject.put("thunder", thunderDisasterDensityArray);
            disasterDensityResultArray.add(disasterDensityResultObject);

            HistoryAnalysisDataEntity recent10YearsDisasterDensity = new HistoryAnalysisDataEntity();
            recent10YearsDisasterDensity.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_DENSITY);
            recent10YearsDisasterDensity.setValue(disasterDensityResultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterDensity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
    * @Description 获取近十年历史典型天气影响事件播报
    * @Author lilin
    * @Create 2017/12/8 12:39
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    //@Scheduled(cron = "*/5 * * * * ?")
    //@PostConstruct
    //@Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsHistoryIncident() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_HISTORY_INCIDENT));

            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL + "GetTypicalCaseInfo/" + getLast10YearDate() + "/" +
                    getLastYearDate();
            JSONObject obj = HttpHelper.getDataByURL(url);

            JSONArray resultArray = new JSONArray();

            JSONArray incidents = (JSONArray) obj.get("Data");
            int size = incidents.size();
            for (int i = 0; i < size; i++) {
                JSONObject incident = (JSONObject) incidents.get(i);
                resultArray.add((String) incident.get("NAME"));
            }

            HistoryAnalysisDataEntity recent10YearsHistoryIncident = new HistoryAnalysisDataEntity();
            recent10YearsHistoryIncident.setName(HistoryAnalysisTaskName.LSSJ_HISTORY_INCIDENT);
            recent10YearsHistoryIncident.setValue(resultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsHistoryIncident);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
    * @Description 统计近10年的灾情类型分析
    * @Author lilin
    * @Create 2017/12/10 11:40
    **/
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    //@Scheduled(cron = "*/5 * * * * ?")
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRecent10YearsDisasterType() {
        try {
            logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TYPE));

            int[] recent10Years = getRecent10Years();

            Map<String, Integer> disasterTypeMap = new HashMap<String, Integer>();
            for (int i = 0; i < 10; i++) {
                String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterDetailData_Geliku/" + getYearStartDate
                        (recent10Years[i]) + "/" + getYearEndDate(recent10Years[i]);
                JSONObject obj = HttpHelper.getDataByURL(url);
                JSONArray disasters = (JSONArray) obj.get("Data");
                int size = disasters.size();

                for (int j = 0; j < size; j++) {
                    JSONObject disaster = (JSONObject) disasters.get(j);
                    String accepter = (String) disaster.get("ACCEPTER");
                    if (accepter != null) {
                        int accepterNum = disasterTypeMap.get(accepter) != null ? disasterTypeMap.get(accepter) : 0;
                        accepterNum++;
                        disasterTypeMap.put(accepter, accepterNum);
                    }
                }
            }
            JSONArray disasterTypeResultArray = new JSONArray();
            int CLSSNum = (int) disasterTypeMap.get("车辆受损") + (int) disasterTypeMap.get("车辆进水") + (int) disasterTypeMap.get("车辆损坏");
            int OthersNum = (int) disasterTypeMap.get("其他") + (int) disasterTypeMap.get("其它");
            int NTSYNum = (int) disasterTypeMap.get("农田受淹") + (int) disasterTypeMap.get("农田积水");
            for (Map.Entry<String, Integer> entry : disasterTypeMap.entrySet()) {
                String key = entry.getKey();
                int value = entry.getValue();
                if (!("车辆受损".equals(key) || "车辆进水".equals(key) || "车辆损坏".equals(key) || "其他".equals(key) || "其它".equals
                        (key) || "农田受淹".equals(key) || "农田积水".equals(key))) {
                    JSONObject disasterTypeResultObject = new JSONObject();
                    disasterTypeResultObject.put("type", key);
                    disasterTypeResultObject.put("value", value);
                    disasterTypeResultArray.add(disasterTypeResultObject);
                }
            }
            JSONObject CLSSObject = new JSONObject();
            JSONObject NTSYObject = new JSONObject();
            JSONObject OthersObject = new JSONObject();
            CLSSObject.put("type", "车辆受损");
            CLSSObject.put("value", CLSSNum);
            OthersObject.put("type", "其他");
            OthersObject.put("value", OthersNum);
            NTSYObject.put("type", "农田受淹");
            NTSYObject.put("value", NTSYNum);
            disasterTypeResultArray.add(CLSSObject);
            disasterTypeResultArray.add(OthersObject);
            disasterTypeResultArray.add(NTSYObject);

            HistoryAnalysisDataEntity recent10YearsDisasterType = new HistoryAnalysisDataEntity();
            recent10YearsDisasterType.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_TYPE);
            recent10YearsDisasterType.setValue(disasterTypeResultArray);

            historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterType);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
