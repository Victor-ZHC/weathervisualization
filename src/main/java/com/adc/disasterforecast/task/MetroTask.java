package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.MetroDataDAO;
import com.adc.disasterforecast.entity.MetroDataEntity;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.MetroTaskName;
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
import java.util.HashMap;
import java.util.Map;

@Component
public class MetroTask {
    // logger for MetroTask
    private static final Logger logger = LoggerFactory.getLogger(MetroTask.class);

    private static Map<Integer, String> lineMapping = new HashMap<>();
    static {
        lineMapping.put("")
    }

    @Autowired
    private MetroDataDAO metroDataDAO;

    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetchGlobalWarning() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WARNING_FORCAST));

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WARNING_FORCAST);
            JSONArray value = new JSONArray();
            metroDataEntity.setValue(value);

            String url = JsonServiceURL.ALARM_JSON_SERVICE_URL
                    + "GetWeatherWarnning";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");

            for (Object o : array) {
                JSONObject input = (JSONObject) o;
                JSONObject output = new JSONObject();
                if (input.get("TYPE").equals("大风")) {
                    output.put("type", "wind");
                    output.put("des", input.get("CONTENT"));
                    if (input.get("LEVEL").equals("红色")) output.put("level", "red");
                    else if (input.get("LEVEL").equals("橙色")) output.put("level", "orange");
                    else if (input.get("LEVEL").equals("黄色")) output.put("level", "yellow");
                    array.add(output);
                } else if (input.get("TYPE").equals("台风")) {
                    output.put("type", "typhoon");
                    output.put("des", input.get("CONTENT"));
                    if (input.get("LEVEL").equals("红色")) output.put("level", "red");
                    else if (input.get("LEVEL").equals("橙色")) output.put("level", "orange");
                    else if (input.get("LEVEL").equals("黄色")) output.put("level", "yellow");
                    array.add(output);
                }
            }

            metroDataDAO.updateMetroDataByName(metroDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetchWeatherForecast() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WEATHER_FORCAST));

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WEATHER_FORCAST);
            JSONArray value = new JSONArray();
            metroDataEntity.setValue(value);
            JSONObject today = new JSONObject();
            value.add(today);

            String url = JsonServiceURL.FORECAST_JSON_SERVICE_URL + "Get10DayForecast";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");
            JSONObject todayJo = (JSONObject) array.get(0);

            today.put("date", DateHelper.getPostponeDateByDay(0));
            today.put("weather", todayJo.get("Day"));
            today.put("minTemp", Integer.parseInt((String) todayJo.get("LowTmp")));
            today.put("maxTemp", Integer.parseInt((String) todayJo.get("HighTmp")));
            today.put("wind", (String) todayJo.get("Wind") + (String) todayJo.get("WindLev"));

            metroDataDAO.updateMetroDataByName(metroDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchLineWindInflunce() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WIND_INFLUENCE));


        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
