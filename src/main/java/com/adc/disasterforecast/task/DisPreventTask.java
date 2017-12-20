package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DisPreventDataDAO;
import com.adc.disasterforecast.entity.DisPreventDataEntity;
import com.adc.disasterforecast.global.DisPreventTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.*;
import com.mongodb.util.JSON;
import org.apache.poi.ss.usermodel.Row;
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
import java.util.*;


/**
 * @author zhichengliu
 * @create -12-06-21:05
 **/
//@Component
public class DisPreventTask {
    // logger for DisPreventTask
    private static final Logger logger = LoggerFactory.getLogger(DisPreventTask.class);

    // dao Autowired
    @Autowired
    private DisPreventDataDAO disPreventDataDAO;

    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void updateJsonData() {
        try {
            JSONObject disasterJsonYears;
            JSONArray disasterDataYears;
            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";
            String endDate = DateHelper.getNow();
            String beginDate = DateHelper.getPostponeDateByYear(endDate, -1);
            String url = baseUrl + beginDate + "/" + endDate;
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            JSONArray disasterData = (JSONArray) disasterJson.get("Data");
            getCurWarning(disasterData);
            endDate = DateHelper.getNow().substring(0, 4) + "0101000000";
            beginDate = DateHelper.getPostponeDateByYear(endDate, -10);
            if (beginDate.compareTo("20160101000000") < 0){
                int baseTime = Calendar.getInstance().get(Calendar.YEAR) - 10;
                beginDate = (baseTime > 2016 ? baseTime : 2016) + "0101000000";
                url = baseUrl + beginDate + "/" + endDate;
                disasterJsonYears = HttpHelper.getDataByURL(url);
                disasterDataYears = (JSONArray) disasterJsonYears.get("Data");
                List<Row> historyWarningFromExcel = ExcelHelper.loadAllExcelFile();
                for (Row row : historyWarningFromExcel) {
                    String content = ExcelHelper.getCellContent(row, 0);
                    if (content.contains("发布") && baseTime <= ExcelHelper.getWarningYear(content)) {
                        String date = ExcelHelper.getWarningDate(content);
                        String type = ExcelHelper.getWarningType(content);
                        JSONObject jsonObject = new JSONObject();
                        jsonObject.put("TYPE", type);
                        jsonObject.put("FORECASTDATE", date);
                        disasterDataYears.add(jsonObject);
                    }
                }

            } else {
                url = baseUrl + beginDate + "/" + endDate;
                disasterJsonYears = HttpHelper.getDataByURL(url);
                disasterDataYears = (JSONArray) disasterJsonYears.get("Data");
            }
            getDisasterAvg(disasterDataYears, disasterData, "大风", DisPreventTaskName.FZJZ_RAINFALL_YEAR);
            getDisasterAvg(disasterDataYears, disasterData, "暴雨", DisPreventTaskName.FZJZ_WIND_YEAR);
            getDisasterAvg(disasterDataYears, disasterData, "雷电", DisPreventTaskName.FZJZ_THUNDER_YEAR);

            baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";
            endDate = DateHelper.getNow();
            beginDate = DateHelper.getPostponeDateByYear(endDate, -1);
            url = baseUrl + beginDate + "/" + endDate;
            disasterJson = HttpHelper.getDataByURL(url);
            disasterData = (JSONArray) disasterJson.get("Data");
            getDisasterType(disasterData);
            getLocation(disasterData);
            beginDate = DateHelper.getPostponeDateByYear(endDate, -10);
            url = baseUrl + beginDate + "/" + endDate;
            disasterJsonYears = HttpHelper.getDataByURL(url);
            disasterDataYears = (JSONArray)disasterJsonYears.get("Data");
            getDisasterCntAvg(disasterDataYears, disasterData);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void getStationData() {
        try {
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
            stationData.put("jishuizhan", waterCnt);

            // YXYB ret null data
            baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYXYB/";
            String endDate = DateHelper.getNow();
            String beginDate = endDate.substring(0, 4) + "0101000000";
            url = baseUrl + beginDate + "/" + endDate;
            int cnt = 486;
//        JSONObject YXYBJson = HttpHelper.getDataByURL(url);
//        JSONArray YXYBData = (JSONArray) YXYBJson.get("Data");
//        cnt = YXYBData.size();
            JSONObject yxybJson = new JSONObject();
            yxybJson.put("total", cnt);
            cnt = 4;
            yxybJson.put("add", cnt);
            stationData.put("yingxiangyubao", yxybJson);

            baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";
            url = baseUrl + beginDate + "/" + endDate;
            JSONObject weatherWarnningJson = HttpHelper.getDataByURL(url);
            JSONArray weatherWarnningData = (JSONArray) weatherWarnningJson.get("Data");
            cnt = weatherWarnningData.size();

            JSONObject fxyjJson = new JSONObject();
            fxyjJson.put("total", cnt);
            stationData.put("zhongduanxitong", (int)((JSONObject)stationData.get("yingxiangyubao")).get("total") + cnt);

            endDate = DateHelper.getNow();
            beginDate = DateHelper.getPostponeDateByDay(endDate, -1);
            url = baseUrl + beginDate + "/" + endDate;
            weatherWarnningJson = HttpHelper.getDataByURL(url);
            weatherWarnningData = (JSONArray) weatherWarnningJson.get("Data");
//            System.out.println(weatherWarnningData);
            cnt = weatherWarnningData.size();
//            cnt = 2;
            fxyjJson.put("add", cnt);

            stationData.put("fengxianyujing", fxyjJson);
            stationData.put("shandiandingweiyi", 4);
            stationData.put("jiaotongjiancedian", 789);
            stationData.put("hangkongjiancedian", 2);
            stationData.put("shipinjiankongdian", 9876);
            beginDate = DateHelper.getPostponeDateByHour("20170101000000", 0);
            stationData.put("duanxin", DateHelper.differentDays(beginDate, endDate));
            stationData.put("weixin", DateHelper.differentDays(beginDate, endDate));
            stationData.put("shehuiguancezhan", 97); // key ??
            stationData.put("app", 54332);

            stationData.put("liandongxiangying", stationData.get("zhongduanxitong"));

            JSONArray valArray = new JSONArray();
            valArray.add(stationData);

            DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
            disPreventDataEntity.setValue(valArray);
            disPreventDataEntity.setName(DisPreventTaskName.FZJZ_DATA_COLLECTION_PLATFORM);
            disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

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

    private void getDisasterAvg(JSONArray disasterDataYears, JSONArray disasterData, String disasterType, String taskName){
        Map<Long, Integer> currentYearVal = new HashMap<>();
        Map<Long, Integer> weekAvgYearVal = new HashMap<>();
        for(Object obj: disasterData) {
            JSONObject disaster = (JSONObject) obj;
//            System.out.println(disaster);
            String month = (String) disaster.get("FORECASTDATE");
            String operation = (String) disaster.get("OPERATION");
//            System.out.println(disaster);
            if (operation != null && operation.compareTo("更新") == 0) continue;
            String type = (String) disaster.get("TYPE");
            if (type.compareTo(disasterType) != 0) continue;
            month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer cnt = currentYearVal.get(monthVal) == null ? 1 : 1 + currentYearVal.get(monthVal);
            currentYearVal.put(monthVal, cnt);
        }
        for(Object obj: disasterDataYears) {
            JSONObject disaster = (JSONObject) obj;
            String operation = (String) disaster.get("OPERATION");
            if (operation != null && operation.compareTo("更新") == 0) continue;
            String type = (String) disaster.get("TYPE");
            if (type.compareTo(disasterType) != 0) continue;
            String month = (String) disaster.get("FORECASTDATE");
            month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
//            System.out.println(month + " " + String.valueOf(type));
            Long monthVal = Long.parseLong(month);
            Integer cnt = weekAvgYearVal.get(monthVal) == null ? 1 : 1 + weekAvgYearVal.get(monthVal);
            weekAvgYearVal.put(monthVal, cnt);
        }

        JSONObject valueObject = new JSONObject();
        JSONArray valueArray = new JSONArray();
        JSONArray currentYearArray = new JSONArray();
        JSONArray weekAvgYearArray = new JSONArray();

        for (int i = 1; i <= 12; i++){
            String baseTime = "2017-";
            if (i < 10) baseTime = baseTime + "0"+ String.valueOf(i) + "-01T00:00:00";
            else baseTime = baseTime + String.valueOf(i) + "-01T00:00:00";
            String month = DateHelper.getFormatWarningMonth(baseTime, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer cnt = weekAvgYearVal.get(monthVal) == null ? 0 : weekAvgYearVal.get(monthVal);
            weekAvgYearVal.put(monthVal, cnt);
            cnt = currentYearVal.get(monthVal) == null ? 0 : currentYearVal.get(monthVal);
            currentYearVal.put(monthVal, cnt);
        }

        // sort
        List<Map.Entry<Long, Integer> > entryList = sortHashMap(currentYearVal);

        for (Map.Entry<Long, Integer> entry: entryList) {
            JSONObject currentYearObject = new JSONObject();
            currentYearObject.put("month", entry.getKey());
            currentYearObject.put("value", entry.getValue());
            currentYearArray.add(currentYearObject);
        }

        entryList = sortHashMap(weekAvgYearVal);
        for (Map.Entry<Long, Integer> entry: entryList) {
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

        List<Map.Entry<Long, Integer> > entrylist = sortHashMap(currentYearVal);

        for (Map.Entry<Long, Integer> entry: entrylist) {
            JSONObject currentYearObject = new JSONObject();
            currentYearObject.put("value", entry.getValue());
            currentYearObject.put("month", entry.getKey());
            currentYearArray.add(currentYearObject);
        }

        entrylist = sortHashMap(weekAvgYearVal);

        for (Map.Entry<Long, Integer> entry: entrylist) {
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
            for (int i =0; i< hashKey.length; i++){
                Double d = Double.parseDouble(hashKey[i]);
                hashKey[i] = String.format("%.2f", d);
            }
            location.put("lat", hashKey[0]);
            location.put("lon", hashKey[1]);
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

        for (int i = 1; i <= 12; i++){
            String baseTime = "2017-";
            if (i < 10) baseTime = baseTime + "0"+ String.valueOf(i) + "-01T00:00:00";
            else baseTime = baseTime + String.valueOf(i) + "-01T00:00:00";
            String month = DateHelper.getFormatWarningMonth(baseTime, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer cnt = hs.get(monthVal) == null ? 0 : hs.get(monthVal);
            hs.put(monthVal, cnt);
        }
        return hs;
    }

    private List<Map.Entry<Long, Integer> > sortHashMap(Map<Long, Integer> hs) {
        List<Map.Entry<Long, Integer> > entryList = new ArrayList<>(hs.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<Long, Integer>>() {
            @Override
            public int compare(Map.Entry<Long, Integer> o1, Map.Entry<Long, Integer> o2) {
                return (o1.getKey()).toString().compareTo(o2.getKey().toString());
            }
        });
        return entryList;
    }

}
