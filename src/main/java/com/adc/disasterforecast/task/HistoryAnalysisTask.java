package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.HistoryAnalysisDataDAO;
import com.adc.disasterforecast.entity.HistoryAnalysisDataEntity;
import com.adc.disasterforecast.global.HistoryAnalysisTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.DateHelper;
import com.adc.disasterforecast.tools.DisasterTypeHelper;
import com.adc.disasterforecast.tools.HttpHelper;
import com.adc.disasterforecast.tools.WarningHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

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
    //@Scheduled(cron = "0 0 0 * * ?")
    @Scheduled(cron = "*/5 * * * * ?")
    public void countRecent10YearsWarnings() {
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
        amountObject.put("total", warnings.size());
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
    @Scheduled(cron = "*/5 * * * * ?")
    public void countRecent10YearsWarningTrend() {
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
    * @Description 统计近10年灾情密度、灾情年度均值、年分布、月分布、日分布、类型分析
    * @Author lilin
    * @Create 2017/12/8 0:07
    **/
    @Scheduled(cron = "*/5 * * * * ?")
    public void countRecent10YearsDisaster() {
        logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_AVG));
        logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_YEAR));
        logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_MONTH));
        logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TREND_DAY));
        logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_DISASTER_TYPE));
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
        int GKZWNum = 0;
        int FWJSNum = 0;
        int NTJSNum = 0;
        int OthersNum = 0;
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

                String desc = (String) disaster.get("CASE_ADDR") + (String) disaster.get("CASE_DESC") + (String)
                        disaster.get("ACCEPTER");
                if ("高空坠物".equals(DisasterTypeHelper.getDisasterType(desc))) {
                    GKZWNum ++;
                }
                if ("房屋进水".equals(DisasterTypeHelper.getDisasterType(desc))) {
                    FWJSNum ++;
                }
                if ("农田进水".equals(DisasterTypeHelper.getDisasterType(desc))) {
                    NTJSNum ++;
                }
                if ("其他".equals(DisasterTypeHelper.getDisasterType(desc))) {
                    OthersNum ++;
                }

                long disasterCode = (long) disaster.get("CODE_DISASTER");
                double lontitude = (double) disaster.get("LONTITUDE");
                double latitude = (double) disaster.get("LATITUDE");
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

        JSONArray disasterTypeResultArray = new JSONArray();
        JSONObject GKZWResultObject = new JSONObject();
        JSONObject FWJSResultObject = new JSONObject();
        JSONObject NTJSResultObject = new JSONObject();
        JSONObject OthersResultObject = new JSONObject();
        GKZWResultObject.put("type", "高空坠物");
        GKZWResultObject.put("value", GKZWNum);
        FWJSResultObject.put("type", "房屋进水");
        FWJSResultObject.put("value", FWJSNum);
        NTJSResultObject.put("type", "农田进水");
        NTJSResultObject.put("value", NTJSNum);
        OthersResultObject.put("type", "其他");
        OthersResultObject.put("value", OthersNum);
        disasterTypeResultArray.add(GKZWResultObject);
        disasterTypeResultArray.add(FWJSResultObject);
        disasterTypeResultArray.add(NTJSResultObject);
        disasterTypeResultArray.add(OthersResultObject);

        HistoryAnalysisDataEntity recent10YearsDisasterType = new HistoryAnalysisDataEntity();
        recent10YearsDisasterType.setName(HistoryAnalysisTaskName.LSSJ_DISASTER_TYPE);
        recent10YearsDisasterType.setValue(disasterTypeResultArray);

        historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsDisasterType);

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
    }

    /**
    * @Description 获取近十年历史典型天气影响事件播报（原始接口暂无数据）
    * @Author lilin
    * @Create 2017/12/8 12:39
    **/
    @Scheduled(cron = "*/5 * * * * ?")
    public void countRecent10YearsHistoryIncident() {
        logger.info(String.format("began task：%s", HistoryAnalysisTaskName.LSSJ_HISTORY_INCIDENT));

        String url = JsonServiceURL.FORECAST_JSON_SERVICE_URL + "GetYXYB/" + getLast10YearDate() + "/" +
                getLastYearDate();
        JSONObject obj = HttpHelper.getDataByURL(url);

        JSONArray resultArray = new JSONArray();

        JSONArray incidents = (JSONArray) obj.get("Data");
        int size = incidents.size();
        for (int i = 0; i < size; i++) {
            JSONObject incident = (JSONObject) incidents.get(i);
            resultArray.add((String) incident.get("SimpleDescription"));
        }

        HistoryAnalysisDataEntity recent10YearsHistoryIncident = new HistoryAnalysisDataEntity();
        recent10YearsHistoryIncident.setName(HistoryAnalysisTaskName.LSSJ_HISTORY_INCIDENT);
        recent10YearsHistoryIncident.setValue(resultArray);

        historyAnalysisDataDAO.updateHistoryAnalysisDataByName(recent10YearsHistoryIncident);
    }
}
