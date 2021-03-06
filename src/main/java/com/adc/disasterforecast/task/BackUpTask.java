package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.BackUpDataDAO;
import com.adc.disasterforecast.entity.BackUpDataEntity;
import com.adc.disasterforecast.global.BackUpDataName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Set;

//@Component
public class BackUpTask {
    // logger for RealTimeControlTask
    private static final Logger logger = LoggerFactory.getLogger(BackUpTask.class);

    // dao Autowired
    @Autowired
    private BackUpDataDAO backUpDataDAO;


//    @PostConstruct
    public void getHistoryRainfall(){
        logger.info(String.format("began task：%s", BackUpDataName.RAINFALL));

        String nowDate = DateHelper.getCurrentTimeInString("day");
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/";

        String beginDate = "";
        String endDate = "";
        int i = 0;
        JSONArray historyRainfallValue = new JSONArray();
        while (! nowDate.equals(endDate)) {
            beginDate = DateHelper.getPostponeDateByDay(2015, 1, 1, 0, 0, 0, i);
            endDate = DateHelper.getPostponeDateByDay(2015, 1, 2, 0, 0, 0, i);

            String url = baseUrl + beginDate + "/" + endDate + "/1";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray autoStationDataArray = (JSONArray) obj.get("Data");

            MaxRainHour max = getMaxRainHour(autoStationDataArray);

            JSONObject maxRainHourByDay = new JSONObject();
            maxRainHourByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
            maxRainHourByDay.put("value", max.getValue());
            maxRainHourByDay.put("siteName", max.getSiteName());
            historyRainfallValue.add(maxRainHourByDay);
            i++;

            BackUpDataEntity historyRainfall = new BackUpDataEntity();
            historyRainfall.setName(BackUpDataName.RAINFALL);
            historyRainfall.setValue(historyRainfallValue);
            backUpDataDAO.updateBackUpDataByName(historyRainfall);
        }

//        BackUpDataEntity historyRainfall = new BackUpDataEntity();
//        historyRainfall.setName(BackUpDataName.RAINFALL);
//        historyRainfall.setValue(historyRainfallValue);
//        backUpDataDAO.updateBackUpDataByName(historyRainfall);
    }

    @PostConstruct
    public void getHistorySeeper(){
        logger.info(String.format("began task：%s", BackUpDataName.SEEPER));

        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYPWaterStationDataByTime/";
        String beginDate = "";
        String endDate = "";

        String nowDate = DateHelper.getCurrentTimeInString("day");

        int i = 0;
        JSONArray historySeeperValue = new JSONArray();
        while (! nowDate.equals(endDate)) {
            beginDate = DateHelper.getPostponeDateByDay(2016, 1, 1, 0, 0, 0, i);
            endDate = DateHelper.getPostponeDateByDay(2016, 1, 2, 0, 0, 0, i);

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray waterStationDataArray = (JSONArray) obj.get("Data");

            if (waterStationDataArray == null) {
                i++;
                continue;
            }

            MaxWaterDepth max = getMaxWaterDepth(waterStationDataArray);

            JSONObject maxWaterDepthByDay = new JSONObject();
            maxWaterDepthByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
            maxWaterDepthByDay.put("value", max.getValue());
            maxWaterDepthByDay.put("siteName", max.getSiteName());
            historySeeperValue.add(maxWaterDepthByDay);
            i++;

            BackUpDataEntity historySeeper = new BackUpDataEntity();
            historySeeper.setName(BackUpDataName.SEEPER);
            historySeeper.setValue(historySeeperValue);
            backUpDataDAO.updateBackUpDataByName(historySeeper);
        }
//        BackUpDataEntity historyRainfall = new BackUpDataEntity();
//        historyRainfall.setName(BackUpDataName.RAINFALL);
//        historyRainfall.setValue(historyRainfallValue);
//        backUpDataDAO.updateBackUpDataByName(historyRainfall);
    }

//    @Scheduled(initialDelay = 86400000, fixedDelay = 86400000)
    @Scheduled(cron = "0 0 0 * * ?")
    public void updateHistoryRainfall(){
        try {
            logger.info(String.format("began update task：%s", BackUpDataName.RAINFALL));
            String nowDate = DateHelper.getCurrentTimeInString("day");
            String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";

            String beginDate = DateHelper.getPostponeDateByDay(nowDate, -1);
            String endDate = nowDate;


            String url = baseUrl + beginDate + "/" + endDate + "/1";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray autoStationDataArray = (JSONArray) obj.get("Data");

            MaxRainHour max = getMaxRainHour(autoStationDataArray);

            JSONArray historyRainfallValue = backUpDataDAO.findBackUpDataByName(BackUpDataName.RAINFALL).getValue();
            JSONObject maxRainHourByDay = new JSONObject();
            maxRainHourByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
            maxRainHourByDay.put("value", max.getValue());
            maxRainHourByDay.put("siteName", max.getSiteName());
            historyRainfallValue.add(maxRainHourByDay);

            BackUpDataEntity historyRainfall = new BackUpDataEntity();
            historyRainfall.setName(BackUpDataName.RAINFALL);
            historyRainfall.setValue(historyRainfallValue);
            backUpDataDAO.updateBackUpDataByName(historyRainfall);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 86400000, fixedDelay = 86400000)
    @Scheduled(cron = "0 0 0 * * ?")
    public void  updateHistorySeeper() {
        try {
            logger.info(String.format("began task：%s", BackUpDataName.SEEPER));

            String nowDate = DateHelper.getCurrentTimeInString("day");

            String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYPWaterStationDataByTime/";
            String beginDate = DateHelper.getPostponeDateByDay(nowDate, -1);
            String endDate = nowDate;

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray waterStationDataArray = (JSONArray) obj.get("Data");

            MaxWaterDepth max = getMaxWaterDepth(waterStationDataArray);

            JSONArray historySeeperValue = backUpDataDAO.findBackUpDataByName(BackUpDataName.SEEPER).getValue();

            JSONObject maxWaterDepthByDay = new JSONObject();
            maxWaterDepthByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
            maxWaterDepthByDay.put("value", max.getValue());
            maxWaterDepthByDay.put("siteName", max.getSiteName());
            historySeeperValue.add(maxWaterDepthByDay);

            BackUpDataEntity historySeeper = new BackUpDataEntity();
            historySeeper.setName(BackUpDataName.SEEPER);
            historySeeper.setValue(historySeeperValue);
            backUpDataDAO.updateBackUpDataByName(historySeeper);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private MaxRainHour getMaxRainHour(JSONArray autoStationDataArray) {
        Set<String> autoStation = StationHelper.getYPAutoStation();
        MaxRainHour maxRainHour = new MaxRainHour();
        for (int j = 0; j < autoStationDataArray.size(); j++) {
            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(j);
            String autoStationId = (String) autoStationData.get("STATIONID");
            double rainHour = RainfallHelper.getRainHour((String) autoStationData.get("RAINHOUR"));
            String siteName = (String) autoStationData.get("STATIONNAME");

            if (autoStation.contains(autoStationId) && rainHour >= maxRainHour.getValue()) {
                maxRainHour.setValue(rainHour);
                maxRainHour.setSiteName(siteName);
            }
        }
        return maxRainHour;
    }

    private MaxWaterDepth getMaxWaterDepth(JSONArray waterStationDataArray) {
        MaxWaterDepth maxWaterDepth = new MaxWaterDepth();
        for (int j = 0; j < waterStationDataArray.size(); j++) {
            JSONObject waterStationData = (JSONObject) waterStationDataArray.get(j);
            double waterDepth = ((Number) waterStationData.get("WATERDEPTH")).doubleValue();
            String siteName = (String) waterStationData.get("STATIONNAME");

            if (waterDepth >= maxWaterDepth.getValue()) {
                maxWaterDepth.setValue(waterDepth);
                maxWaterDepth.setSiteName(siteName);
            }
        }
        return maxWaterDepth;
    }

//    @PostConstruct
//    public void localContinueSeeper() {
//        logger.info(String.format("began task：%s", BackUpDataName.SEEPER));
//
//        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYPWaterStationDataByTime/";
//        String beginDate = "";
//        String endDate = "";
//
//        String nowDate = DateHelper.getCurrentTimeInString("day");
//
//        int i = 0;
//        JSONArray historySeeperValue = backUpDataDAO.findBackUpDataByName(BackUpDataName.SEEPER).getValue();
//        while (! nowDate.equals(endDate)) {
//            beginDate = DateHelper.getPostponeDateByDay(2017, 3, 12, 0, 0, 0, i);
//            endDate = DateHelper.getPostponeDateByDay(2017, 3, 13, 0, 0, 0, i);
//
//            String url = baseUrl + beginDate + "/" + endDate;
//
//            JSONObject obj = HttpHelper.getDataByURL(url);
//            JSONArray waterStationDataArray = (JSONArray) obj.get("Data");
//
//            if (waterStationDataArray == null) {
//                i++;
//                continue;
//            }
//
//            double max = getMaxWaterDepth(waterStationDataArray);
//
//            JSONObject maxWaterDepthByDay = new JSONObject();
//            maxWaterDepthByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
//            maxWaterDepthByDay.put("value", max);
//            historySeeperValue.add(maxWaterDepthByDay);
//            i++;
//
//            BackUpDataEntity historySeeper = new BackUpDataEntity();
//            historySeeper.setName(BackUpDataName.SEEPER);
//            historySeeper.setValue(historySeeperValue);
//            backUpDataDAO.updateBackUpDataByName(historySeeper);
//        }
//    }
}

class MaxRainHour {
    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    private String siteName = "";
    private double value = 0.0;

}

class MaxWaterDepth {
    public String getSiteName() {
        return siteName;
    }

    public void setSiteName(String siteName) {
        this.siteName = siteName;
    }

    public double getValue() {
        return value;
    }

    public void setValue(double value) {
        this.value = value;
    }

    private String siteName = "";
    private double value = 0.0;
}