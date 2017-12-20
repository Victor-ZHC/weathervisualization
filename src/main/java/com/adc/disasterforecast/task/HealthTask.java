package com.adc.disasterforecast.task;


import com.adc.disasterforecast.dao.HealthDataDAO;
import com.adc.disasterforecast.entity.HealthDataEntity;
import com.adc.disasterforecast.entity.po.HistoryHealthData;
import com.adc.disasterforecast.global.HealthTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.DateHelper;
import com.adc.disasterforecast.tools.HttpHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.adc.disasterforecast.task.HealthTask.AirQualityType.*;


@Component
public class HealthTask {
    // logger for HealthTask
    private static final Logger logger = LoggerFactory.getLogger(HealthTask.class);

    @Autowired
    private HealthDataDAO healthDataDAO;

    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetchWeatherForecast() {
        try {
            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_WEATHER_FORCAST));

            HealthDataEntity healthDataEntity = new HealthDataEntity();
            healthDataEntity.setName(HealthTaskName.KPI_JKQX_WEATHER_FORCAST);
            JSONArray value = new JSONArray();
            healthDataEntity.setValue(value);
            JSONObject today = new JSONObject();
            JSONObject tomorrow = new JSONObject();
            value.add(today);
            value.add(tomorrow);

            String url = JsonServiceURL.FORECAST_JSON_SERVICE_URL + "Get10DayForecast";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");
            JSONObject todayJo = (JSONObject) array.get(0);
            JSONObject tomorrowJo = (JSONObject) array.get(1);

            today.put("date", DateHelper.getPostponeDateByDay(0));
            today.put("weather", todayJo.get("Day"));
            today.put("minTemp", Integer.parseInt((String) todayJo.get("LowTmp")));
            today.put("maxTemp", Integer.parseInt((String) todayJo.get("HighTmp")));
            today.put("wind", (String) todayJo.get("Wind") + (String) todayJo.get("WindLev"));

            tomorrow.put("date", DateHelper.getPostponeDateByDay(1));
            tomorrow.put("weather", tomorrowJo.get("Day"));
            tomorrow.put("minTemp", Integer.parseInt((String) tomorrowJo.get("LowTmp")));
            tomorrow.put("maxTemp", Integer.parseInt((String) tomorrowJo.get("HighTmp")));
            tomorrow.put("wind", (String) tomorrowJo.get("Wind") + (String) tomorrowJo.get("WindLev"));

            healthDataDAO.updateHealthDataByName(healthDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchAQI() {
        try {
            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_AIR_QUALITY));

            HealthDataEntity healthDataEntity = new HealthDataEntity();
            healthDataEntity.setName(HealthTaskName.KPI_JKQX_AIR_QUALITY);
            JSONArray value = new JSONArray();
            healthDataEntity.setValue(value);
            JSONObject realtimeData = new JSONObject();
            value.add(realtimeData);

            // 获取实时AQI数据
            String url = String.format("%s%s/%s",
                    JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL,
                    "GetLastest_SHAQI_Realtime",
                    DateHelper.getNow());

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");
            JSONObject jo = (JSONObject) array.get(0);

            JSONObject aqiRealtime = new JSONObject();
            int aqi = (int) (long) jo.get("AQI");
            aqiRealtime.put("value", aqi);
            aqiRealtime.put("level", airQualityLevel(aqi, AQI));
            JSONObject pm25Realtime = new JSONObject();
            int pm25 = (int)((double)jo.get("PM2_5"));
            pm25Realtime.put("value", pm25);
            pm25Realtime.put("level", airQualityLevel(pm25, PM25));
            JSONObject pm10Realtime = new JSONObject();
            int pm10 = (int)((double)jo.get("PM10"));
            pm10Realtime.put("value", pm10);
            pm10Realtime.put("level", airQualityLevel(pm10, PM10));
            JSONObject no2Realtime = new JSONObject();
            int no2 = (int)((double)jo.get("NO2"));
            no2Realtime.put("value", no2);
            no2Realtime.put("level", airQualityLevel(no2, NO2));
            JSONObject so2Realtime = new JSONObject();
            int so2 = (int)((double)jo.get("SO2"));
            so2Realtime.put("value", so2);
            so2Realtime.put("level", airQualityLevel(so2, SO2));
            JSONObject o3Realtime = new JSONObject();
            int o3 = (int)((double)jo.get("O3"));
            o3Realtime.put("value", o3);
            o3Realtime.put("level", airQualityLevel(o3, O3));

            realtimeData.put("date", new Date().getTime());
            realtimeData.put("AQI", aqiRealtime);
            realtimeData.put("PM25", pm25Realtime);
            realtimeData.put("PM10", pm10Realtime);
            realtimeData.put("O3", o3Realtime);
            realtimeData.put("SO2", so2Realtime);
            realtimeData.put("NO2", no2Realtime);

            // 获取未来AQI数据
            url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL + "GetAirQuality";
            obj = HttpHelper.getDataByURL(url);
            JSONObject data = (JSONObject) obj.get("Data");
            array = (JSONArray) data.get("AQIDatas");

            int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
            int startIndex, endIndex;
            if (hour >=6 && hour < 17) {
                startIndex = 2;
                endIndex = 5;
            } else {
                startIndex = 1;
                endIndex = 4;
            }

            for (int i = startIndex; i < endIndex; ++i) {
                JSONObject aqiData = (JSONObject) array.get(i);
                String period = (String) aqiData.get("Period");
                if (period.contains("（"))
                    period = period.substring(0, period.indexOf("（"));
                String aqiStr = (String) aqiData.get("AQI");
                int futureAqi = aqiStr.contains("-")
                        ? Integer.parseInt(aqiStr.substring(aqiStr.indexOf("-") + 1))
                        : Integer.parseInt(aqiStr);
                int level = airQualityLevel(futureAqi, AQI);
                JSONObject futureData = new JSONObject();
                futureData.put("date", period);
                JSONObject aqiInfo = new JSONObject();
                aqiInfo.put("level", level);
                aqiInfo.put("value", futureAqi);
                futureData.put("AQI", aqiInfo);
                value.add(futureData);
            }

            healthDataDAO.updateHealthDataByName(healthDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }


    @PostConstruct
    @Scheduled(cron = "0 0 1 * * ?")
    public void fetchHistoryHealthyMeteorological() {
        try {
            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_HISTORY_DISEASE));

            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL + "Getlastesthealthweather";
            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");
            String beginTime = "9999/99/99 9:99:99";
            for (Object o : array) {
                JSONObject jo = (JSONObject) o;
                HistoryHealthData healthData = new HistoryHealthData(jo);
                beginTime = healthData.FORECAST_TIME;
                healthDataDAO.upsertHistoryHealthData(healthData);
            }

            // 2017/12/18 0:00:00 -> 2017/12
            beginTime = beginTime.substring(0, 7);

            // 历史数据更新完毕，现在需要根据历史数据计算本月数据
            List<HistoryHealthData> historyHealthDataList = healthDataDAO.findHistoryHealthData(beginTime);
            int copdCount = 0;
            int childFluCount = 0;
            int childAsthmaCount = 0;
            int oldFluCount = 0;
            int adultFluCount = 0;
            for (HistoryHealthData data : historyHealthDataList) {
                if (data.WARNING_LEVEL > 4) {
                    if (data.CROW.equals("COPD患者")) ++copdCount;
                    else if (data.CROW.equals("儿童感冒")) ++childFluCount;
                    else if (data.CROW.equals("儿童哮喘")) ++childAsthmaCount;
                    else if (data.CROW.equals("老年人感冒")) ++oldFluCount;
                    else if (data.CROW.equals("青少年和成年人感冒")) ++adultFluCount;
                }
            }

            int total = copdCount + childFluCount + childAsthmaCount + oldFluCount + adultFluCount;
            int copdPercent = total == 0 ? 0 : Math.round((float) (copdCount * 100) / total);
            int childFluPercent = total == 0 ? 0 : Math.round((float) (childFluCount * 100) / total);
            int childAsthmaPercent = total == 0 ? 0 : Math.round((float) (childAsthmaCount * 100) / total);
            int oldFluPercent = total == 0 ? 0 : Math.round((float) (oldFluCount * 100) / total);
            int adultFluPercent = total == 0 ? 0 : Math.round((float) (adultFluCount * 100) / total);

            JSONObject jo = new JSONObject();
            jo.put("ertong", childFluPercent);
            jo.put("qingshaonian", adultFluPercent);
            jo.put("laonianren", oldFluPercent);
            jo.put("ertongxiaochuan", childAsthmaPercent);
            jo.put("COPD", copdPercent);
            JSONArray value = new JSONArray();
            value.add(jo);

            HealthDataEntity healthDataEntity = new HealthDataEntity();
            healthDataEntity.setName(HealthTaskName.KPI_JKQX_HISTORY_DISEASE);
            healthDataEntity.setValue(value);
            healthDataDAO.updateHealthDataByName(healthDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchHealthyMeteorologicalInTodayAndTomorrow() {
        try {
            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_HEALTHY_FORCAST));

            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL + "GetHealthyMeteorological";
            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");

            HealthDataEntity healthDataEntity = new HealthDataEntity();
            healthDataEntity.setName(HealthTaskName.KPI_JKQX_HEALTHY_FORCAST);
            JSONArray value = new JSONArray();
            healthDataEntity.setValue(value);

            Calendar today = Calendar.getInstance();
            today.set(Calendar.HOUR_OF_DAY, 0);
            today.set(Calendar.MINUTE, 0);
            today.set(Calendar.SECOND, 0);
            today.set(Calendar.MILLISECOND, 0);
            Calendar tomorrow = (Calendar) today.clone();
            tomorrow.add(Calendar.DAY_OF_YEAR, 1);

            JSONObject todayData = new JSONObject();
            JSONObject tomorrowData = new JSONObject();
            value.add(todayData);
            value.add(tomorrowData);

            todayData.put("date", today.getTimeInMillis());
            tomorrowData.put("date", tomorrow.getTimeInMillis());

            array.forEach(o -> {
                JSONObject jo = (JSONObject) o;
                if (jo.get("Crow").equals("COPD患者")) {
                    fillHealthyMeteorological("COPD", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("儿童感冒")) {
                    fillHealthyMeteorological("ertong", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("儿童哮喘")) {
                    fillHealthyMeteorological("ertongxiaochuan", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("老年人感冒")) {
                    fillHealthyMeteorological("laonianren", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("青少年和成年人感冒")) {
                    fillHealthyMeteorological("qingshaonian", jo, todayData, tomorrowData);
                }
            });

            healthDataDAO.updateHealthDataByName(healthDataEntity);

            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_HEALTHY_FORCAST_SPREAD));

            healthDataEntity.setName(HealthTaskName.KPI_JKQX_HEALTHY_FORCAST_SPREAD);
            todayData.clear();
            tomorrowData.clear();
            todayData.put("date", today.getTimeInMillis());
            tomorrowData.put("date", tomorrow.getTimeInMillis());

            array.forEach(o -> {
                JSONObject jo = (JSONObject) o;
                if (jo.get("Crow").equals("COPD患者")) {
                    fillHealthyMeteorologicalInCity("COPD", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("儿童感冒")) {
                    fillHealthyMeteorologicalInCity("ertong", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("儿童哮喘")) {
                    fillHealthyMeteorologicalInCity("ertongxiaochuan", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("老年人感冒")) {
                    fillHealthyMeteorologicalInCity("laonianren", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("青少年和成年人感冒")) {
                    fillHealthyMeteorologicalInCity("qingshaonian", jo, todayData, tomorrowData);
                }
            });
            healthDataDAO.updateHealthDataByName(healthDataEntity);

            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_PREVENT_ADVICE));

            healthDataEntity.setName(HealthTaskName.KPI_JKQX_PREVENT_ADVICE);
            todayData.clear();
            tomorrowData.clear();
            todayData.put("date", today.getTimeInMillis());
            tomorrowData.put("date", tomorrow.getTimeInMillis());

            array.forEach(o -> {
                JSONObject jo = (JSONObject) o;
                if (jo.get("Crow").equals("COPD患者")) {
                    fillHealthyMeteorologicalAdvice("COPD", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("儿童感冒")) {
                    fillHealthyMeteorologicalAdvice("ertong", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("儿童哮喘")) {
                    fillHealthyMeteorologicalAdvice("ertongxiaochuan", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("老年人感冒")) {
                    fillHealthyMeteorologicalAdvice("laonianren", jo, todayData, tomorrowData);
                } else if (jo.get("Crow").equals("青少年和成年人感冒")) {
                    fillHealthyMeteorologicalAdvice("qingshaonian", jo, todayData, tomorrowData);
                }
            });
            healthDataDAO.updateHealthDataByName(healthDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @PostConstruct
//    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchServicePublish() {
        try {
            logger.info(String.format("began task：%s", HealthTaskName.KPI_JKQX_SERVICE_PUBLISH));

            String now = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
            String url = JsonServiceURL.HEALTH_SEND_NUM_URL + now;

            JSONArray array = HttpHelper.getJsonDataByURL(url);
            int ftpCount = Integer.parseInt((String) ((JSONObject)array.get(0)).get("FTP"));
            int smsCount = Integer.parseInt((String) ((JSONObject)array.get(0)).get("短信"));
            int emailCount = Integer.parseInt((String) ((JSONObject)array.get(0)).get("邮件"));
            int wsCount = 74126;

            JSONArray value = new JSONArray();
            JSONObject returnData = new JSONObject();
            returnData.put("mail", emailCount);
            returnData.put("message", smsCount);
            returnData.put("ftp", ftpCount);
            returnData.put("webservice", wsCount);
            value.add(returnData);

            HealthDataEntity healthDataEntity = new HealthDataEntity();
            healthDataEntity.setName(HealthTaskName.KPI_JKQX_SERVICE_PUBLISH);
            healthDataEntity.setValue(value);

            healthDataDAO.updateHealthDataByName(healthDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void fillHealthyMeteorological(String userType, JSONObject jo,
                                           JSONObject todayData, JSONObject tomorrowData) {
        JSONArray details = (JSONArray) jo.get("Deatails");
        int todayLevel = Integer.parseInt((String)((JSONObject) details.get(0)).get("WarningLevel"));
        JSONObject todayLevelData = new JSONObject();
        todayLevelData.put("level", todayLevel);
        todayData.put(userType, todayLevelData);

        int tomorrowLevel = Integer.parseInt((String)((JSONObject) details.get(1)).get("WarningLevel"));
        JSONObject tomorrowLevelData = new JSONObject();
        tomorrowLevelData.put("level", tomorrowLevel);
        tomorrowData.put(userType, tomorrowLevelData);
    }

    private void fillHealthyMeteorologicalInCity(String userType, JSONObject jo,
                                           JSONObject todayData, JSONObject tomorrowData) {
        JSONArray details = (JSONArray) jo.get("Deatails");

        int todayLevel = Integer.parseInt((String)((JSONObject) details.get(0)).get("WarningLevel"));
        JSONArray todayCityArray = new JSONArray();
        fillHealthyMeteorologicalCityArray(todayLevel, todayCityArray);
        todayData.put(userType, todayCityArray);

        int tomorrowLevel = Integer.parseInt((String)((JSONObject) details.get(1)).get("WarningLevel"));
        JSONArray tomorrowCityArray = new JSONArray();
        fillHealthyMeteorologicalCityArray(tomorrowLevel, tomorrowCityArray);
        tomorrowData.put(userType, tomorrowCityArray);
    }

    private void fillHealthyMeteorologicalAdvice(String userType, JSONObject jo,
                                           JSONObject todayData, JSONObject tomorrowData) {
        JSONArray details = (JSONArray) jo.get("Deatails");
        int todayLevel = Integer.parseInt((String)((JSONObject) details.get(0)).get("WarningLevel"));
        String todayPrevent = (String)((JSONObject) details.get(0)).get("Influ");
        String todayDes = (String)((JSONObject) details.get(0)).get("Wat_guide");
        JSONObject todayLevelData = new JSONObject();
        todayLevelData.put("level", todayLevel);
        todayLevelData.put("prevent", todayPrevent);
        todayLevelData.put("des", todayDes);
        todayData.put(userType, todayLevelData);

        int tomorrowLevel = Integer.parseInt((String)((JSONObject) details.get(1)).get("WarningLevel"));
        String tomorrowPrevent = (String)((JSONObject) details.get(1)).get("Wat_guide");
        String tomorrowDes = (String)((JSONObject) details.get(1)).get("Wat_guide");
        JSONObject tomorrowLevelData = new JSONObject();
        tomorrowLevelData.put("level", tomorrowLevel);
        tomorrowLevelData.put("prevent", tomorrowPrevent);
        tomorrowLevelData.put("des", tomorrowDes);
        tomorrowData.put(userType, tomorrowLevelData);
    }

    private void fillHealthyMeteorologicalCityArray(int centerLevel, JSONArray array) {
        fillHealthyMeteorologicalForDistrict("zhongxinchengqu", centerLevel, array);
        fillHealthyMeteorologicalForDistrict("pudongxinqu", 0, array);
        fillHealthyMeteorologicalForDistrict("fengxian", 0, array);
        fillHealthyMeteorologicalForDistrict("baoshan", 0, array);
        fillHealthyMeteorologicalForDistrict("minhang", 0, array);
        fillHealthyMeteorologicalForDistrict("jinshan", 0, array);
        fillHealthyMeteorologicalForDistrict("songjiang", 0, array);
        fillHealthyMeteorologicalForDistrict("qingpu", 0, array);
        fillHealthyMeteorologicalForDistrict("jiading", 0, array);
        fillHealthyMeteorologicalForDistrict("chongming", 0, array);
    }

    private void fillHealthyMeteorologicalForDistrict(String districtName, int level, JSONArray array) {
        JSONObject area = new JSONObject();
        area.put(districtName, level);
        array.add(area);
    }

    enum AirQualityType {
        AQI,
        PM25,
        PM10,
        O3,
        NO2,
        SO2
    }

    private int airQualityLevel(int value, AirQualityType type) {
        switch (type) {
            case AQI:
                if (value < 50) return 1;
                else if (value < 100) return 2;
                else if (value < 150) return 3;
                else if (value < 200) return 4;
                else if (value < 300) return 5;
                else return 6;
            case PM25:
                if (value < 35) return 1;
                else if (value < 75) return 2;
                else if (value < 115) return 3;
                else if (value < 150) return 4;
                else if (value < 250) return 5;
                else return 6;
            case PM10:
                if (value < 50) return 1;
                else if (value < 150) return 2;
                else if (value < 250) return 3;
                else if (value < 350) return 4;
                else if (value < 420) return 5;
                else return 6;
            case SO2:
                if (value < 150) return 1;
                else if (value < 500) return 2;
                else if (value < 650) return 3;
                else return 4;
            case NO2:
                if (value < 100) return 1;
                else if (value < 200) return 2;
                else if (value < 700) return 3;
                else if (value < 1200) return 4;
                else if (value < 2340) return 5;
                else return 6;
            case O3:
                if (value < 160) return 1;
                else if (value < 200) return 2;
                else if (value < 300) return 3;
                else if (value < 400) return 4;
                else if (value < 800) return 5;
                else return 6;
        }
        return 1;
    }
}
