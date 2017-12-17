package com.adc.disasterforecast.task;


import com.adc.disasterforecast.dao.HealthDataDAO;
import com.adc.disasterforecast.entity.HealthDataEntity;
import com.adc.disasterforecast.global.HealthTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
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


@Component
public class HealthTask {
    // logger for HealthTask
    private static final Logger logger = LoggerFactory.getLogger(HealthTask.class);

    @Autowired
    private HealthDataDAO healthDataDAO;

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchHealthyMeteorologicalInTodayAndTomorrow() {
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
    }

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchServicePublish() {
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
        fillHealthyMeteorologicalForDistrict("pudongxinqu", 1, array);
        fillHealthyMeteorologicalForDistrict("fengxian", 1, array);
        fillHealthyMeteorologicalForDistrict("baoshan", 1, array);
        fillHealthyMeteorologicalForDistrict("minhang", 1, array);
        fillHealthyMeteorologicalForDistrict("jinshan", 1, array);
        fillHealthyMeteorologicalForDistrict("songjiang", 1, array);
        fillHealthyMeteorologicalForDistrict("qingpu", 1, array);
        fillHealthyMeteorologicalForDistrict("jiading", 1, array);
        fillHealthyMeteorologicalForDistrict("chongming", 1, array);
    }

    private void fillHealthyMeteorologicalForDistrict(String districtName, int level, JSONArray array) {
        JSONObject area = new JSONObject();
        area.put(districtName, level);
        array.add(area);
    }

}
