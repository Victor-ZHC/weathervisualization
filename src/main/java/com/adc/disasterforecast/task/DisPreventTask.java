package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DisPreventDataDAO;
import com.adc.disasterforecast.entity.DisPreventDataEntity;
import com.adc.disasterforecast.global.DisPreventTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.DateHelper;
import com.adc.disasterforecast.tools.HttpHelper;
import com.adc.disasterforecast.tools.WarningHelper;
import com.adc.disasterforecast.tools.DisasterTypeHelper;
import com.mongodb.util.JSON;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.aggregation.ArrayOperators;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Map;


/**
 * @author zhichengliu
 * @create -12-06-21:05
 **/
@Component
public class DisPreventTask {
    // logger for DisPreventTask
    private static final Logger logger = LoggerFactory.getLogger(DisPreventTask.class);

    // dao Autowired
    @Autowired
    private DisPreventDataDAO disPreventDataDAO;

    @PostConstruct
    public void updateJsonData() {
        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";
        String endDate = DateHelper.getNow();
        String beginDate = DateHelper.getPostponeDateByYear(endDate, -1);
        String url = baseUrl + beginDate + "/" + endDate;
        JSONObject disasterJson = HttpHelper.getDataByURL(url);
        JSONArray disasterData = (JSONArray) disasterJson.get("Data");
        getCurWarning(disasterData);
        baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";
        url = baseUrl + beginDate + "/" + endDate;
        disasterJson = HttpHelper.getDataByURL(url);
        disasterData = (JSONArray) disasterJson.get("Data");
        getDisasterType(disasterData);
        getLocation(disasterData);
        endDate = DateHelper.getNow().substring(0, 4) + "0101000000";
        beginDate = DateHelper.getPostponeDateByYear(endDate, -10);
        url = baseUrl + beginDate + "/" + endDate;
        JSONObject disasterJsonYears = HttpHelper.getDataByURL(url);
        JSONArray disasterDataYears = (JSONArray) disasterJsonYears.get("Data");
        getDisasterAvg(disasterDataYears, disasterData, 1, DisPreventTaskName.FZJZ_RAINFALL_YEAR);
        getDisasterAvg(disasterDataYears, disasterData, 2, DisPreventTaskName.FZJZ_WIND_YEAR);
        getDisasterAvg(disasterDataYears, disasterData, 3, DisPreventTaskName.FZJZ_THUNDER_YEAR);
        getDisasterCntAvg(disasterDataYears, disasterData);

    }

    @PostConstruct
    public void getStationData() {
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterStationData/";
        String date = DateHelper.getNow();
        String url = baseUrl + date;
        JSONObject weatherStationJson = HttpHelper.getDataByURL(url);
        JSONArray weatherStationData = (JSONArray) weatherStationJson.get("Data");
        int weatherCnt = weatherStationData.size();
        JSONObject stationData = new JSONObject();
        stationData.put("qixiangzidongzhan", weatherCnt);

        baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterOut_Geliku/";
        url = baseUrl + date;
        JSONObject waterOutStationJson = HttpHelper.getDataByURL(url);
        JSONArray waterOutStationData = (JSONArray) waterOutStationJson.get("Data");
        int waterOutCnt = waterOutStationData.size();
        stationData.put("shuiwenzhan", waterOutCnt);

        baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterStationData_Geliku/";
        url = baseUrl + date;
        JSONObject waterStationJson = HttpHelper.getDataByURL(url);
        JSONArray waterStationData = (JSONArray) waterStationJson.get("Data");
        int waterCnt = waterStationData.size();
        stationData.put("shuiwenzhan", waterCnt);

        // YXYB ret null data
        baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYXYB/";
        String endDate = DateHelper.getNow();
        String beginDate = endDate.substring(0, 4) + "0101000000";
        url = baseUrl + beginDate + "/" + endDate;
        int cnt;
//        JSONObject YXYBJson = HttpHelper.getDataByURL(url);
//        JSONArray YXYBData = (JSONArray) YXYBJson.get("Data");
//        cnt = YXYBData.size();
//        stationData.put("yingxiangyubao", cnt);

        baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";
        url = baseUrl + beginDate + "/" + endDate;
        JSONObject weatherWarnningJson = HttpHelper.getDataByURL(url);
        JSONArray weatherWarnningData = (JSONArray) weatherWarnningJson.get("Data");
        cnt = weatherWarnningData.size();
        stationData.put("fengxianyujing", cnt);
    }

    private void getCurWarning(JSONArray curWarningData) {
        Map<String, Integer> amountMap = new HashMap<>();
        Map<String, Integer> levelMap = new HashMap<>();

        for (Object obj: curWarningData) {
            JSONObject curWarning = (JSONObject) obj;
            String level = WarningHelper.getWarningLevel((String) curWarning.get("LEVEL"));
            String type = WarningHelper.getWarningWeather((String) curWarning.get("TYPE"));
            Integer cnt = levelMap.get(level) == null ? 1 : levelMap.get(level) + 1;
            levelMap.put(level, cnt);
            if (type.compareTo("") == 0) continue;
            cnt = amountMap.get("total") == null ? 1 : 1 + amountMap.get("total");
            amountMap.put("total", cnt);
            cnt = amountMap.get(type) == null ? 1 : amountMap.get(type) + 1;
            amountMap.put(type, cnt);
        }

        JSONObject valData = new JSONObject();
        JSONArray valueData = new JSONArray();

        valData.put("amount", amountMap);
        valData.put("level", levelMap);
        valueData.add(valData);

        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setName(DisPreventTaskName.FZJZ_WARNING_YEAR);
        disPreventDataEntity.setValue(valueData);
        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);
    }

    private void getDisasterType(JSONArray disasterData) {
        Map<String, Integer> disasterMap = new HashMap<>();
        Map<String, Integer> rainDisasterMap = new HashMap<>();
        Map<String, Integer> windDisasterMap = new HashMap<>();
        Map<String, Integer> thunderDisasterMap = new HashMap<>();
        for (Object obj : disasterData) {
            JSONObject disaster = (JSONObject) obj;
            int codeDisaster = ((Long) disaster.get("CODE_DISASTER")).intValue();
            String typeDetail = (String) disaster.get("ACCEPTER");
            Integer cnt;
            if (codeDisaster == 1) {
                cnt = rainDisasterMap.get(typeDetail) == null ? 1 : 1 + rainDisasterMap.get(typeDetail);
                rainDisasterMap.put(typeDetail, cnt);
            } else if (codeDisaster == 2){
                cnt = windDisasterMap.get(typeDetail) == null ? 1 : 1 + windDisasterMap.get(typeDetail);
                windDisasterMap.put(typeDetail, cnt);
            } else if (codeDisaster == 3){
                cnt = thunderDisasterMap.get(typeDetail) == null ? 1 : 1 + thunderDisasterMap.get(typeDetail);
                thunderDisasterMap.put(typeDetail, cnt);
            } else continue;

            cnt = disasterMap.get("total") == null ? 1 : 1 + disasterMap.get("total");
            disasterMap.put("total", cnt);
            String type = WarningHelper.getWarningWeather(DisasterTypeHelper.CODE_DISASTER[codeDisaster]);
            cnt = disasterMap.get(type) == null ? 1 : 1 + disasterMap.get(type);
            disasterMap.put(type, cnt);
        }

        JSONArray valArray = new JSONArray();
        JSONObject valObject = new JSONObject();
        JSONArray rainArray = new JSONArray();
        JSONArray windArray = new JSONArray();
        JSONArray thunderArray = new JSONArray();
        JSONObject thunderObject;
        for (Map.Entry<String, Integer> entry: rainDisasterMap.entrySet()) {
            JSONObject rainObject = new JSONObject();
            rainObject.put("type", entry.getKey());
            rainObject.put("value", entry.getValue());
            rainArray.add(rainObject);
        }
        for (Map.Entry<String, Integer> entry: windDisasterMap.entrySet()) {
            JSONObject windObject = new JSONObject();
            windObject.put("value", entry.getValue());
            windObject.put("type", entry.getKey());
            windArray.add(windObject);
        }
        for (Map.Entry<String, Integer> entry: thunderDisasterMap.entrySet()) {
            thunderObject = new JSONObject();
            thunderObject.put("value", entry.getValue());
            thunderObject.put("type", entry.getKey());
            thunderArray.add(thunderObject);
        }
        valObject.put("rain", rainArray);
        valObject.put("wind", windArray);
        valObject.put("thunder", thunderArray);
        valArray.add(valObject);

        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setValue(valArray);
        disPreventDataEntity.setName(DisPreventTaskName.FZJZ_DISASTER_TYPE_YEAR);
        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);

        valArray = new JSONArray();
        valArray.add(disasterMap);
        disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setName(DisPreventTaskName.FZJZ_DISASTER_YEAR);
        disPreventDataEntity.setValue(valArray);
        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);

    }

    private void getDisasterAvg(JSONArray disasterDataYears, JSONArray disasterData, int disasterType, String taskName){
        Map<Long, Integer> currentYearVal = new HashMap<>();
        Map<Long, Integer> weekAvgYearVal = new HashMap<>();
        for(Object obj: disasterData) {
            JSONObject disaster = (JSONObject) obj;
            String month = (String) disaster.get("DATETIME_DISASTER");
            int type = ((Long) disaster.get("CODE_DISASTER")).intValue();
            if (type != disasterType) continue;
            month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer cnt = currentYearVal.get(monthVal) == null ? 1 : 1 + currentYearVal.get(monthVal);
            currentYearVal.put(monthVal, cnt);
        }
        for(Object obj: disasterDataYears) {
            JSONObject disaster = (JSONObject) obj;
            int type = ((Long) disaster.get("CODE_DISASTER")).intValue();
            if (type != disasterType) continue;
            String month = (String) disaster.get("DATETIME_DISASTER");
            month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer cnt = weekAvgYearVal.get(monthVal) == null ? 1 : 1 + weekAvgYearVal.get(monthVal);
            weekAvgYearVal.put(monthVal, cnt);
        }

        JSONObject valueObject = new JSONObject();
        JSONArray valueArray = new JSONArray();
        JSONArray currentYearArray = new JSONArray();
        JSONArray weekAvgYearArray = new JSONArray();

        for (Map.Entry<Long, Integer> entry: currentYearVal.entrySet()) {
            JSONObject currentYearObject = new JSONObject();
            currentYearObject.put("month", entry.getKey());
            currentYearObject.put("value", entry.getValue());
            currentYearArray.add(currentYearObject);
        }

        for (Map.Entry<Long, Integer> entry: weekAvgYearVal.entrySet()) {
            JSONObject weekAvgYearObject = new JSONObject();
            weekAvgYearObject.put("value", Double.parseDouble(entry.getValue().toString()) / 10);
            weekAvgYearObject.put("month", entry.getKey());
            weekAvgYearArray.add(weekAvgYearObject);
        }
        valueObject.put("currentYear", currentYearArray);
        valueObject.put("weekavgYear", weekAvgYearArray);
        valueArray.add(valueObject);

        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setName(taskName);
        disPreventDataEntity.setValue(valueArray);
        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);
    }

    private void getDisasterCntAvg(JSONArray disasterDataYears, JSONArray disasterData) {
        Map<Long, Integer> currentYearVal = getDisasterCntByJson(disasterData);
        Map<Long, Integer> weekAvgYearVal = getDisasterCntByJson(disasterDataYears);

        JSONArray currentYearArray = new JSONArray();
        JSONArray weekAvgYearArray = new JSONArray();
        JSONObject valueObject = new JSONObject();
        JSONArray valueArray = new JSONArray();

        for (Map.Entry<Long, Integer> entry: currentYearVal.entrySet()) {
            JSONObject currentYearObject = new JSONObject();
            currentYearObject.put("value", entry.getValue());
            currentYearObject.put("month", entry.getKey());
            currentYearArray.add(currentYearObject);
        }

        for (Map.Entry<Long, Integer> entry: weekAvgYearVal.entrySet()) {
            JSONObject weekAvgYearObject = new JSONObject();
            weekAvgYearObject.put("month", entry.getKey());
            weekAvgYearObject.put("value", Double.parseDouble(entry.getValue().toString()) / 10);
            weekAvgYearArray.add(weekAvgYearObject);
        }
        valueObject.put("currentYear", currentYearArray);
        valueObject.put("weekavgYear", weekAvgYearArray);
        valueArray.add(valueObject);

        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setName(DisPreventTaskName.FZJZ_DISASTER_SPREAD_YEAR);
        disPreventDataEntity.setValue(valueArray);
        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);

    }

    private void getLocation(JSONArray disasterData) {
        Map<String, Integer> locationMap = new HashMap<>();
        for (Object obj: disasterData) {
            JSONObject disaster = (JSONObject) obj;
            Double latitude = (Double) disaster.get("LATITUDE");
            Double longitude = (Double) disaster.get("LONTITUDE");
            String hashKey = latitude + " " + longitude;
            Integer cnt = locationMap.get(hashKey) == null ? 1 : 1 + locationMap.get(hashKey);
            locationMap.put(hashKey, cnt);
        }

        JSONArray locationVal = new JSONArray();
        for (Map.Entry<String, Integer> entry: locationMap.entrySet()) {
            JSONObject location = new JSONObject();
            String [] hashKey = entry.getKey().split(" ");
            location.put("lat", Double.parseDouble(hashKey[0]));
            location.put("lon", Double.parseDouble(hashKey[1]));
            location.put("value", entry.getValue());
            locationVal.add(location);
        }

        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setName(DisPreventTaskName.FZJZ_DISASTER_SPACE_SPREAD_YEAR);
        disPreventDataEntity.setValue(locationVal);
        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);
    }

    private Map<Long, Integer> getDisasterCntByJson(JSONArray disasterData) {
        Map<Long, Integer> hs = new HashMap<>();
        for(Object obj: disasterData) {
            JSONObject disaster = (JSONObject) obj;
            String month = (String) disaster.get("DATETIME_DISASTER");
            month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer cnt = hs.get(monthVal) == null ? 1 : 1 + hs.get(monthVal);
            hs.put(monthVal, cnt);
        }
        return hs;
    }

}