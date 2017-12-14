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

import java.util.Map;
import java.util.Set;

@Component
public class BackUpTask {
    // logger for RealTimeControlTask
    private static final Logger logger = LoggerFactory.getLogger(BackUpTask.class);

    // dao Autowired
    @Autowired
    private BackUpDataDAO backUpDataDAO;


//    @EventListener(ApplicationReadyEvent.class)
//    public void getHistoryRainfall(){
//        logger.info(String.format("began task：%s", BackUpDataName.RAINFALL));
//
//        String nowDate = DateHelper.getCurrentTimeInString("day");
//        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/";
//
//        String beginDate = "";
//        String endDate = "";
//        int i = 0;
//        JSONArray historyRainfallValue = new JSONArray();
//        while (! nowDate.equals(endDate)) {
//            beginDate = DateHelper.getPostponeDateByDay(2015, 1, 1, 0, 0, 0, i);
//            endDate = DateHelper.getPostponeDateByDay(2015, 1, 2, 0, 0, 0, i);
//
//            String url = baseUrl + beginDate + "/" + endDate + "/1";
//
//            JSONObject obj = HttpHelper.getDataByURL(url);
//            JSONArray autoStationDataArray = (JSONArray) obj.get("Data");
//
//            double max = getMaxRainHour(autoStationDataArray);
//
//            JSONObject maxRainHourByDay = new JSONObject();
//            maxRainHourByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
//            maxRainHourByDay.put("value", max);
//            historyRainfallValue.add(maxRainHourByDay);
//            i++;
//
////            BackUpDataEntity historyRainfall = new BackUpDataEntity();
////            historyRainfall.setName(BackUpDataName.RAINFALL);
////            historyRainfall.setValue(historyRainfallValue);
////            backUpDataDAO.updateBackUpDataByName(historyRainfall);
//        }
//
//        BackUpDataEntity historyRainfall = new BackUpDataEntity();
//        historyRainfall.setName(BackUpDataName.RAINFALL);
//        historyRainfall.setValue(historyRainfallValue);
//        backUpDataDAO.updateBackUpDataByName(historyRainfall);
//    }

    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    public void updateHistoryRainfall(){
        logger.info(String.format("began update task：%s", BackUpDataName.RAINFALL));
        String nowDate = DateHelper.getCurrentTimeInString("day");
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/";

        String beginDate = DateHelper.getPostponeDateByDay(nowDate, -1);
        String endDate = nowDate;


        String url = baseUrl + beginDate + "/" + endDate + "/1";

        JSONObject obj = HttpHelper.getDataByURL(url);
        JSONArray autoStationDataArray = (JSONArray) obj.get("Data");

        double max = getMaxRainHour(autoStationDataArray);

        JSONArray historyRainfallValue = backUpDataDAO.findBackUpDataByName(BackUpDataName.RAINFALL).getValue();
        JSONObject maxRainHourByDay = new JSONObject();
        maxRainHourByDay.put("date", DateHelper.getPostponeDateByHourInLong(beginDate, 0));
        maxRainHourByDay.put("value", max);
        historyRainfallValue.add(maxRainHourByDay);

        BackUpDataEntity historyRainfall = new BackUpDataEntity();
        historyRainfall.setName(BackUpDataName.RAINFALL);
        historyRainfall.setValue(historyRainfallValue);
        backUpDataDAO.updateBackUpDataByName(historyRainfall);
    }

    private double getMaxRainHour(JSONArray autoStationDataArray) {
        Set<String> autoStation = StationHelper.getYPAutoStation();
        double max = 0.0;
        for (int j = 0; j < autoStationDataArray.size(); j++) {
            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(j);
            String autoStationId = (String) autoStationData.get("STATIONID");
            double rainHour = RainfallHelper.getRainHour((String) autoStationData.get("RAINHOUR"));

            if (autoStation.contains(autoStationId) && rainHour > max) {
                max = rainHour;
            }
        }
        return max;
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
//    public void updateHistoryWarning(){
//        logger.info(String.format("began update task：%s", BackUpDataName.WARNING));
//        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRiskAlarmByUsername/ypq";
//
//        JSONObject obj = HttpHelper.getDataByURL(url);
//        JSONObject warningObj = (JSONObject) obj.get("Data");
//        JSONArray subWarnings = (JSONArray) warningObj.get("SubAlarms");
//
//        JSONArray historyWarning = backUpDataDAO.findBackUpDataByName(BackUpDataName.WARNING).getValue();
//
//        for (int i = 0; i < subWarnings.size(); i++) {
//            JSONObject subWarning = (JSONObject) subWarnings.get(i);
//
//            if (! "解除".equals(subWarning.get("OPERATION"))) {
//                String name = (String) subWarning.get("Name");
//                String date = (String) subWarning.get("FORECASTDATE");
//                int level = WarningHelper.getWarningInInt((String) subWarning.get("LEVEL"));
//
//
//            }
//
//        }
//
//    }
}
