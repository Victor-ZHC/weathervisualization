package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DisPreventDataDAO;
import com.adc.disasterforecast.dao.HistoryAnalysisDataDAO;
import com.adc.disasterforecast.dao.WeatherDayDAO;
import com.adc.disasterforecast.entity.DisPreventDataEntity;
import com.adc.disasterforecast.entity.po.WeatherDay;
import com.adc.disasterforecast.global.DisPrventRegionName;
import com.adc.disasterforecast.entity.HistoryAnalysisDataEntity;
import com.adc.disasterforecast.global.DisPreventTaskName;
import com.adc.disasterforecast.global.HistoryAnalysisTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.LocalDataPath;
import com.adc.disasterforecast.tools.*;
import org.apache.poi.ss.usermodel.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import javax.annotation.PostConstruct;
import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;


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
    @Autowired
    private HistoryAnalysisDataDAO historyAnalysisDataDAO;
    @Autowired
    private WeatherDayDAO weatherDayDAO;

//    @PostConstruct
//    @Scheduled(cron = "0 0 0 * * ?")
    public void updateJsonData() {
        try {
            JSONObject disasterJsonYears;
            JSONArray disasterDataYears;
            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";
            String endDate = DateHelper.getNow();
            String beginDate = endDate.substring(0, 4) + "0101000000";
            String url = baseUrl + beginDate + "/" + endDate;
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            JSONArray disasterData = (JSONArray) disasterJson.get("Data");

            getCurWarning(disasterData);

//            getDisasterAvg("大风", DisPreventTaskName.FZJZ_WIND_YEAR);
//            getDisasterAvg("暴雨", DisPreventTaskName.FZJZ_RAINFALL_YEAR);
            getNewDisasterAvg("大风", DisPreventTaskName.FZJZ_WIND_YEAR);
            getNewDisasterAvg("暴雨", DisPreventTaskName.FZJZ_RAINFALL_YEAR);
            getDisasterAvg("雷电", DisPreventTaskName.FZJZ_THUNDER_YEAR);

            baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";
            endDate = DateHelper.getNow();
            beginDate = endDate.substring(0, 4) + "0101000000";
            url = baseUrl + beginDate + "/" + endDate;
            disasterJson = HttpHelper.getDataByURL(url);
            disasterData = (JSONArray) disasterJson.get("Data");
            getDisasterType(disasterData);
            getLocation(disasterData);

            beginDate = DateHelper.getPostponeDateByMonth(endDate, -11);
            url = baseUrl + beginDate + "/" + endDate;
            disasterJson = HttpHelper.getDataByURL(url);
            disasterData = (JSONArray) disasterJson.get("Data");

            beginDate = DateHelper.getPostponeDateByYear(endDate, -10);
            url = baseUrl + beginDate + "/" + endDate;
            disasterJsonYears = HttpHelper.getDataByURL(url);
            disasterDataYears = (JSONArray)disasterJsonYears.get("Data");
            getDisasterCntAvg(disasterDataYears, disasterData);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public static void main(String[] args) {
        new DisPreventTask().updateRainHistoryData();
    }

    //    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void getStationData() {
        try {
            String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";
            String dateEnd = DateHelper.getCurrentTimeInString("hour");
            String dateBegin = DateHelper.getPostponeDateByHour(dateEnd, -1);
            String url = baseUrl + dateBegin + "/" + dateEnd + "/1";
            JSONObject weatherStationJson = HttpHelper.getDataByURL(url);
            JSONArray weatherStationData = (JSONArray) weatherStationJson.get("Data");
            int weatherCnt = weatherStationData.size();
            JSONObject stationData = new JSONObject();
            stationData.put("qixiangzidongzhan", weatherCnt);

            String date = DateHelper.getNow();
            baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterOut_Geliku/";
            url = baseUrl + date;
            JSONObject waterOutStationJson = HttpHelper.getDataByURL(url);
            JSONArray waterOutStationData = (JSONArray) waterOutStationJson.get("Data");
            int waterOutCnt = waterOutStationData == null ? 0 : waterOutStationData.size();
            stationData.put("shuiwenzhan", waterOutCnt);

            baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterStationData/";
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
            int cnt = 0;
//            JSONObject YXYBJson = HttpHelper.getDataByURL(url);
//            JSONArray YXYBData = (JSONArray) YXYBJson.get("Data");
//            cnt = YXYBData.size();
            JSONObject yxybJson = new JSONObject();
            yxybJson.put("total", cnt);
            cnt = 0;
            yxybJson.put("add", cnt);
            stationData.put("yingxiangyubao", yxybJson);

            baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRiskAlarmByTime/";
            url = baseUrl + beginDate + "/" + endDate;
            JSONObject weatherWarnningJson = HttpHelper.getDataByURL(url);
            JSONArray weatherWarnningData = (JSONArray) weatherWarnningJson.get("Data");
//            cnt = weatherWarnningData.size();
            // 计算年度风险预警
            Set<String> yearRiskAlert = new HashSet<>();
            for (int i = 0; i < weatherWarnningData.size(); ++i) {
                JSONObject jo = (JSONObject) weatherWarnningData.get(i);
                if ("发布".equals(jo.get("State"))) {
                    yearRiskAlert.add((String) jo.get("ForecastTime"));
                }
            }
            cnt = yearRiskAlert.size();

            JSONObject fxyjJson = new JSONObject();
            fxyjJson.put("total", cnt);
            stationData.put("zhongduanxitong", (int)((JSONObject)stationData.get("yingxiangyubao")).get("total") + cnt);

            endDate = DateHelper.getNow();
            beginDate = endDate.substring(0, 8) + "000000";
            url = baseUrl + beginDate + "/" + endDate;
            weatherWarnningJson = HttpHelper.getDataByURL(url);
            weatherWarnningData = (JSONArray) weatherWarnningJson.get("Data");
//            cnt = weatherWarnningData.size();
            // 计算当日风险预警
            Set<String> dayRiskAlert = new HashSet<>();
            for (int i = 0; i < weatherWarnningData.size(); ++i) {
                JSONObject jo = (JSONObject) weatherStationData.get(i);
                if ("发布".equals(jo.get("State"))) {
                    dayRiskAlert.add((String) jo.get("ForecastTime"));
                }
            }
            cnt = dayRiskAlert.size();
            fxyjJson.put("add", cnt);

            baseUrl = JsonServiceURL.VERIFY_USER_URL + "GetMessageCount/";
            beginDate = endDate.substring(0, 4) + "0101000000";
            url = baseUrl + beginDate + "/" + endDate;
            weatherStationJson = HttpHelper.getDataByURL(url);
            stationData.put("duanxin", weatherStationJson.get("Data"));

            stationData.put("fengxianyujing", fxyjJson);
            stationData.put("shandiandingweiyi", 4);
            stationData.put("jiaotongjiancedian", 32);
            stationData.put("hangkongjiancedian", 2);
            stationData.put("shipinjiankongdian", 9876);

            beginDate = DateHelper.getPostponeDateByHour(DateHelper.getNow().substring(0, 4) + "0101000000", 0);


            Calendar today = Calendar.getInstance();

            stationData.put("shehuiguancezhan", 97); // key ??

            cnt = DateHelper.differentDays(beginDate, endDate).intValue();
            if("180000".compareTo(endDate.substring(8, 14)) <= 0) cnt++;

            stationData.put("app", cnt);
            stationData.put("weixin", cnt);
//            System.out.println(cnt);

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

        for (Object obj : curWarningData) {
            JSONObject curWarning = (JSONObject) obj;

            // 如果不是风雨雷，就跳过
            if (!WarningHelper.TYPE_WIND.equals(curWarning.get("TYPE"))
                    && !WarningHelper.TYPE_RAIN.equals(curWarning.get("TYPE"))
                    && !WarningHelper.TYPE_THUNDER.equals(curWarning.get("TYPE")))
                continue;
            // 如果不是红橙黄蓝，就跳过
            if (!WarningHelper.LEVEL_BLUE.equals(curWarning.get("LEVEL"))
                    && !WarningHelper.LEVEL_ORANGE.equals(curWarning.get("LEVEL"))
                    && !WarningHelper.LEVEL_RED.equals(curWarning.get("LEVEL"))
                    && !WarningHelper.LEVEL_YELLOW.equals(curWarning.get("LEVEL")))
                continue;

            String level = WarningHelper.getWarningLevel((String) curWarning.get("LEVEL"));
            String type = WarningHelper.getWarningWeather((String) curWarning.get("TYPE"));
            Integer cnt = levelMap.get(level) == null ? 1 : levelMap.get(level) + 1;
            levelMap.put(level, cnt);
            cnt = amountMap.get("total") == null ? 1 : 1 + amountMap.get("total");
            amountMap.put("total", cnt);
            cnt = amountMap.get(type) == null ? 1 : amountMap.get(type) + 1;
            amountMap.put(type, cnt);
        }

        HistoryAnalysisDataEntity historyAnalysisDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName(HistoryAnalysisTaskName.LSSJ_WARNING_YEAR);
        List<Object> hisValue = historyAnalysisDataEntity.getValue();
        Map<String, Object> hisAmountMap = (Map<String, Object>) ((Map<String, Object>) (hisValue.get(0))).get("amount");
        int hisTotal = (int) hisAmountMap.get("total");
        int yearAvg = (int) Math.round(hisTotal / 10.0);
        amountMap.put("yearAvg", yearAvg);

//        int yearAvg = getPastTenYearWarning();
//        amountMap.put("yearAvg", yearAvg);

        JSONObject valData = new JSONObject();
        JSONArray valueData = new JSONArray();

        valData.put("amount", amountMap);
        valData.put("level", levelMap);
        valueData.add(valData);

        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
        disPreventDataEntity.setName(DisPreventTaskName.FZJZ_WARNING_YEAR);
        disPreventDataEntity.setValue(valueData);
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
            if (codeDisaster == DisasterTypeHelper.DISASTER_RAIN_CODE) {
                cnt = rainDisasterMap.get(typeDetail) == null ? 1 : 1 + rainDisasterMap.get(typeDetail);
                rainDisasterMap.put(typeDetail, cnt);
            } else if (codeDisaster == DisasterTypeHelper.DISASTER_WIND_CODE){
                cnt = windDisasterMap.get(typeDetail) == null ? 1 : 1 + windDisasterMap.get(typeDetail);
                windDisasterMap.put(typeDetail, cnt);
            } else if (codeDisaster == DisasterTypeHelper.DISASTER_THUNDER_CODE){
                cnt = thunderDisasterMap.get(typeDetail) == null ? 1 : 1 + thunderDisasterMap.get(typeDetail);
                thunderDisasterMap.put(typeDetail, cnt);
            }

            cnt = disasterMap.get("total") == null ? 1 : 1 + disasterMap.get("total");
            disasterMap.put("total", cnt);
            String type = DisasterTypeHelper.getDisasterTypeByCode(codeDisaster);
            if ("".equals(type)) type = DisasterTypeHelper.DISASTER_OTHER;
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

    @PostConstruct
    public void updateRainHistoryData() {
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei";
        LocalDateTime startTime = LocalDateTime.of(2018, 1, 1, 20,0,0);
        LocalDateTime endTime = LocalDateTime.of(2018, 1, 10, 20,0,0);
        for (LocalDateTime timeIter = startTime; timeIter.compareTo(endTime) < 0; timeIter = timeIter.plusDays(1)) {
            Map<String, Float> station2Value = new HashMap<>();
            station2Value.put("闵行", 0f);
            station2Value.put("宝山", 0f);
            station2Value.put("嘉定", 0f);
            station2Value.put("崇明", 0f);
            station2Value.put("徐家汇", 0f);
            station2Value.put("惠南", 0f);
            station2Value.put("浦东", 0f);
            station2Value.put("金山", 0f);
            station2Value.put("青浦", 0f);
            station2Value.put("松江", 0f);
            station2Value.put("奉贤", 0f);
            for (int i = 0; i < 12 * 24; ++i) {
                LocalDateTime fiveMinuteIter = timeIter.plusMinutes(i * 5);
                String url = String.format("%s/%s/%s/1", baseUrl,
                        DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(fiveMinuteIter),
                        DateTimeFormatter.ofPattern("yyyyMMddHHmmss").format(fiveMinuteIter.plusMinutes(5)));

                JSONObject json = HttpHelper.getDataByURL(url);
                JSONArray dataJson = (JSONArray) json.get("Data");
                for (int j = 0; j < dataJson.size(); ++j) {
                    JSONObject oneData = (JSONObject) dataJson.get(j);
                    String stationName = (String) oneData.get("STATIONNAME");
                    if (!station2Value.containsKey(stationName)) continue;
                    String rainStr = (String) oneData.get("RAINHOUR");
                    float rain = Float.valueOf(rainStr);
                    station2Value.put(stationName, station2Value.get(stationName) + rain);
                }
            }
            WeatherDay weatherDay = new WeatherDay();
            weatherDay.setYear(timeIter.getYear());
            weatherDay.setMonth(timeIter.getMonthValue());
            weatherDay.setDay(timeIter.getDayOfYear());
            weatherDay.setType("rain");
            weatherDay.setValue(0);
            for (String stationName : station2Value.keySet()) {
                if (station2Value.get(stationName) > 50) {
                    weatherDay.setValue(1);
                    break;
                }
            }
            weatherDayDAO.upsertWeatherDay(weatherDay);
            logger.info("weather day finished: "
                    + DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(timeIter));
        }
    }

    private void getNewDisasterAvg(String disasterType, String taskName){
//        JSONObject valueObject = new JSONObject();
//        JSONArray valueArray = new JSONArray();
//        JSONArray currentYearArray = new JSONArray();
//        JSONArray weekAvgYearArray = new JSONArray();
//
//        for (Map.Entry<Long, Integer> entry: entryList) {
//            JSONObject currentYearObject = new JSONObject();
//            currentYearObject.put("month", entry.getKey());
//            currentYearObject.put("value", entry.getValue());
//            currentYearArray.add(currentYearObject);
//        }
//
//        List<Map.Entry<Long, Double> > entryList_tmp = sortDoubleHashMap(weekAvgYearVal);
//        for (Map.Entry<Long, Double> entry: entryList_tmp) {
//            JSONObject weekAvgYearObject = new JSONObject();
//            weekAvgYearObject.put("value", Double.parseDouble(entry.getValue().toString()) / year);
//            weekAvgYearObject.put("month", entry.getKey());
//            weekAvgYearArray.add(weekAvgYearObject);
//        }
//        valueObject.put("currentYear", currentYearArray);
//        valueObject.put("weekavgYear", weekAvgYearArray);
//        valueArray.add(valueObject);
//
//        DisPreventDataEntity disPreventDataEntity = new DisPreventDataEntity();
//        disPreventDataEntity.setName(taskName);
//        disPreventDataEntity.setValue(valueArray);
//        disPreventDataDAO.updateDisPreventDataByName(disPreventDataEntity);
    }

    private void getDisasterAvg(String disasterType, String taskName){
        // TODO: filepath edit
//        String csvFilePath = "/root/weather_visualization_crawler/file_output/wind";
//        String csvFilePath = "G:\\work\\crawler\\file_output\\wind";
        Map<Long, Integer> currentYearVal;
        Map<Long, Double> weekAvgYearVal;
        int year = 10;
        if(WarningHelper.TYPE_WIND.equals(disasterType)) {
//            currentYearVal = getDisasterCurYearByCsvFile(disasterType, csvFilePath);
            currentYearVal = getDisasterCurYear(disasterType);
            weekAvgYearVal = getHistory("wind_rain_08_17", disasterType);
        }
        else if (WarningHelper.TYPE_RAIN.equals(disasterType)) {
            currentYearVal = getDisasterCurYear(disasterType);
            weekAvgYearVal = getHistory("wind_rain_08_17", disasterType);
        }
        else{
            currentYearVal = getThunderCurYear();
            weekAvgYearVal = getThunderHisroty();
//            weekAvgYearVal = getHistory("thunder_08_14", disasterType);
//            getThunderHistory(weekAvgYearVal, "20140101000000");
        }
//        System.out.println(currentYearVal);
//        System.out.println(weekAvgYearVal);
//        Map<Long, Integer> weekAvgYearVal = new HashMap<>();
//        for(Object obj: disasterData) {
//            JSONObject disaster = (JSONObject) obj;
////            System.out.println(disaster);
//            String month = (String) disaster.get("FORECASTDATE");
//            String operation = (String) disaster.get("OPERATION");
////            System.out.println(disaster);
//            if (operation != null && operation.compareTo("更新") == 0) continue;
//            String type = (String) disaster.get("TYPE");
//            if (type.compareTo(disasterType) != 0) continue;
//            month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
//            Long monthVal = Long.parseLong(month);
//            Integer cnt = currentYearVal.get(monthVal) == null ? 1 : 1 + currentYearVal.get(monthVal);
//            currentYearVal.put(monthVal, cnt);
//        }
//        for(Object obj: disasterDataYears) {
//            JSONObject disaster = (JSONObject) obj;
//            String operation = (String) disaster.get("OPERATION");
//            if("更新".equals(operation)) continue;
//            String type = (String) disaster.get("TYPE");
//            if (!type.equals(disasterType)) continue;
//            String month = (String) disaster.get("FORECASTDATE");
//            if(DateHelper.getMonth(month).compareTo(DateHelper.getNow().substring(4, 6)) > 0)
//                month = DateHelper.getFormatWarningMonth(month, DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4));
//            else
//                month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
////            if(month.substring(4, 6).compareTo(DateHelper.getNow().substring(4, 6)) > 0)
////                DateHelper.getPostponeDateByYear(month, -1);
////            System.out.println(month + " " + String.valueOf(type));
//            Long monthVal = Long.parseLong(month);
//            Integer cnt = weekAvgYearVal.get(monthVal) == null ? 1 : 1 + weekAvgYearVal.get(monthVal);
//            weekAvgYearVal.put(monthVal, cnt);
//        }
//        addLocalData(weekAvgYearVal, "wind_rain_17", disasterType, DateHelper.getPostponeDateByYear(DateHelper.getNow(), -2));
//        addLocalData(currentYearVal, "wind_rain_17", disasterType, DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1));

        JSONObject valueObject = new JSONObject();
        JSONArray valueArray = new JSONArray();
        JSONArray currentYearArray = new JSONArray();
        JSONArray weekAvgYearArray = new JSONArray();

        for(int i = 0; i < 12; i++){
            String BaseTime = DateHelper.getNow().substring(0, 6) + "01000000";
            String month = null;
            try {
                month = DateHelper.getPostponeDateByMonth(BaseTime, -i);
            }catch (Exception e){
                e.printStackTrace();
            }
            Long monthVal = Long.parseLong(DateHelper.getTimeMillis(month));
            Double cnt = weekAvgYearVal.get(monthVal) == null ? 0.0 : weekAvgYearVal.get(monthVal);
            weekAvgYearVal.put(monthVal, cnt);
            Integer cnt_tmp = currentYearVal.get(monthVal) == null ? 0 : currentYearVal.get(monthVal);
            currentYearVal.put(monthVal, cnt_tmp);
        }

//        for (int i = 1; i <= 12; i++){
//            String baseTime = DateHelper.getNow().substring(0, 4) + "-";
//            if (i < 10) baseTime = baseTime + "0"+ String.valueOf(i) + "-01T00:00:00";
//            else baseTime = baseTime + String.valueOf(i) + "-01T00:00:00";
//            String month = DateHelper.getFormatWarningMonth(baseTime, DateHelper.getNow().substring(0, 4));
//            Long monthVal = Long.parseLong(month);
//            Integer cnt = weekAvgYearVal.get(monthVal) == null ? 0 : weekAvgYearVal.get(monthVal);
//            weekAvgYearVal.put(monthVal, cnt);
//            cnt = currentYearVal.get(monthVal) == null ? 0 : currentYearVal.get(monthVal);
//            currentYearVal.put(monthVal, cnt);
//        }
//        System.out.println(currentYearVal);
        // sort
        List<Map.Entry<Long, Integer> > entryList = sortIntegerHashMap(currentYearVal);

        for (Map.Entry<Long, Integer> entry: entryList) {
            JSONObject currentYearObject = new JSONObject();
            currentYearObject.put("month", entry.getKey());
            currentYearObject.put("value", entry.getValue());
            currentYearArray.add(currentYearObject);
        }

        List<Map.Entry<Long, Double> > entryList_tmp = sortDoubleHashMap(weekAvgYearVal);
        for (Map.Entry<Long, Double> entry: entryList_tmp) {
            JSONObject weekAvgYearObject = new JSONObject();
            weekAvgYearObject.put("value", Double.parseDouble(entry.getValue().toString()) / year);
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

        List<Map.Entry<Long, Integer> > entrylist = sortIntegerHashMap(currentYearVal);

        for (Map.Entry<Long, Integer> entry: entrylist) {
            JSONObject currentYearObject = new JSONObject();
            currentYearObject.put("value", entry.getValue());
            currentYearObject.put("month", entry.getKey());
            currentYearArray.add(currentYearObject);
        }

        entrylist = sortIntegerHashMap(weekAvgYearVal);

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
            String year = Calendar.getInstance().get(Calendar.MONTH) + 1 >= Integer.parseInt(month.substring(5, 7))
                    ? String.valueOf(Calendar.getInstance().get(Calendar.YEAR))
                    : String.valueOf(Calendar.getInstance().get(Calendar.YEAR) - 1);
            month = DateHelper.getFormatWarningMonth(month, year);
            Long monthVal = Long.parseLong(month);
            Integer cnt = hs.get(monthVal) == null ? 1 : 1 + hs.get(monthVal);
            hs.put(monthVal, cnt);
        }

        for (int i = 1; i <= 12; i++){
            String baseTime = DateHelper.getNow().substring(0, 4) + "-";
            if (i < 10) baseTime = baseTime + "0"+ String.valueOf(i) + "-01T00:00:00";
            else baseTime = baseTime + String.valueOf(i) + "-01T00:00:00";
            String year = Calendar.getInstance().get(Calendar.MONTH) + 1 >= i
                    ? String.valueOf(Calendar.getInstance().get(Calendar.YEAR))
                    : String.valueOf(Calendar.getInstance().get(Calendar.YEAR) - 1);
            String month = DateHelper.getFormatWarningMonth(baseTime, year);
            Long monthVal = Long.parseLong(month);
            Integer cnt = hs.get(monthVal) == null ? 0 : hs.get(monthVal);
            hs.put(monthVal, cnt);
        }
        return hs;
    }

    private List<Map.Entry<Long, Double> > sortDoubleHashMap(Map<Long, Double> hs) {
        List<Map.Entry<Long, Double> > entryList = new ArrayList<>(hs.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<Long, Double>>() {
            @Override
            public int compare(Map.Entry<Long, Double> o1, Map.Entry<Long, Double> o2) {
                return (o1.getKey()).toString().compareTo(o2.getKey().toString());
            }
        });
        return entryList;
    }

    private List<Map.Entry<Long, Integer> > sortIntegerHashMap(Map<Long, Integer> hs) {
        List<Map.Entry<Long, Integer> > entryList = new ArrayList<>(hs.entrySet());
        Collections.sort(entryList, new Comparator<Map.Entry<Long, Integer>>() {
            @Override
            public int compare(Map.Entry<Long, Integer> o1, Map.Entry<Long, Integer> o2) {
                return (o1.getKey()).toString().compareTo(o2.getKey().toString());
            }
        });
        return entryList;
    }

    private Map<Long, Integer> getDisasterCurYear(String disasterType) {
        Map<Long, Integer> currentYearVal = new HashMap<>();
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";
        String endDate = DateHelper.getNow().substring(0, 8) + "000000";
        String beginDate = endDate;
        String stopDate = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1);
        while (true) {
            int cnt = 0;
            endDate = beginDate;
            beginDate =  DateHelper.getPostponeDateByHour(endDate, -1);
            if(beginDate.compareTo(stopDate) < 0) break;
            String url = baseUrl + beginDate + "/" + endDate + "/1";
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            if(disasterJson == null || disasterJson.get("Data") == null) continue;
            JSONArray disasterData = (JSONArray) disasterJson.get("Data");
            if(disasterData == null || disasterData.size() < 1) continue;
            String month = DateHelper.getFormatDate((String)((JSONObject)disasterData.get(0)).get("DATETIME"));
            if(DateHelper.getNow().substring(4, 6).compareTo(month.substring(4, 6)) < 0)
                month = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4) + month.substring(4, 6) + "01000000";
            else
                month = DateHelper.getNow().substring(0, 4) + month.substring(4, 6) + "01000000";
            Long monthVal = Long.parseLong(DateHelper.getTimeMillis(month));
            for (Object obj: disasterData) {
                JSONObject disaster = (JSONObject) obj;
                boolean isok = false;
                for(int i = 0; i < DisPrventRegionName.BASE_STATION_NAME.length; i++){
                    if(DisPrventRegionName.BASE_STATION_NAME[i].equals(disaster.get("STATIONNAME"))){
                        isok = true;
                        break;
                    }
                }
                if(!isok) continue;
                if(WarningHelper.TYPE_WIND.equals(disasterType)) {
                    String speedData = (String)disaster.get("WINDSPEED");
                    if("".equals(speedData)) continue;
                    Double speed = Double.parseDouble(speedData);
                    if (speed > DisasterTypeHelper.WIND_THRESHOLD) cnt++;
                }else if (WarningHelper.TYPE_RAIN.equals(disasterType)) {
                    Double rainhour = Double.parseDouble((String)disaster.get("RAINHOUR"));
                    if (rainhour > DisasterTypeHelper.RAIN_THRESHOLD) cnt++;
                }
            }
            Integer num = currentYearVal.get(monthVal) == null ? 0 : currentYearVal.get(monthVal);
            if(cnt > 0) num++;
            currentYearVal.put(monthVal, num);
        }
        return currentYearVal;
    }

    private Map<Long, Integer> getDisasterCurYearByCsvFile(String disasterType, String inputCsvDir) {
        Map<Long, Integer> currentYearVal = new HashMap<>();
        try {
            File csvDir = new File(inputCsvDir);
            File [] allCsvFile = csvDir.listFiles();
            if (allCsvFile == null) return null;
            for(File csvFile: allCsvFile) {
                JSONArray csvData = CsvHelper.parseCsvFile(csvFile.getCanonicalPath());
                System.out.println(csvFile.getName());
                if(csvData == null) {
                    logger.info("csvFile: " + csvFile.getCanonicalPath() + " has nothing.");
                    continue;
                }
                String month = csvFile.getName().split("-")[0];
                if(DateHelper.getNow().substring(4, 6).compareTo(month.substring(4, 6)) < 0)
                    month = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4) + month.substring(4, 6) + "01000000";
                else
                    month = DateHelper.getNow().substring(0, 4) + month.substring(4, 6) + "01000000";
                Long monthVal = Long.parseLong(DateHelper.getTimeMillis(month));
                Double maxVal = Double.MIN_VALUE;
                String type = WarningHelper.TYPE_RAIN.compareTo(disasterType) == 0 ? "RAINHOUR" : "WINDSPEED";
                Double threshold = WarningHelper.TYPE_RAIN.compareTo(disasterType) == 0 ?
                        DisasterTypeHelper.RAIN_THRESHOLD : DisasterTypeHelper.WIND_THRESHOLD;
                for (Object obj: csvData) {
                    JSONObject item = (JSONObject) obj;
                    if (item.get(type) == null) continue;
                    String stationName = (String) item.get("STATIONNAME");
                    boolean isok = false;
                    for (String baseStationName: DisPrventRegionName.BASE_STATION_NAME){
                        if (baseStationName.compareTo(stationName) == 0 && (isok = true)) break;
                    }
                    if(!isok) continue;
                    Double tmp = Double.parseDouble((String) item.get(type));
                    maxVal = Math.max(maxVal, tmp);
                    if(maxVal > threshold) break;
                }
                System.out.println(maxVal.toString() + " " + csvFile.getName().split("-")[0] + " " + threshold.toString());
                Integer num = currentYearVal.get(monthVal) == null ? 0 : currentYearVal.get(monthVal);
                if(maxVal > threshold) num++;
                currentYearVal.put(monthVal, num);
                System.out.println(currentYearVal);
            }
            return currentYearVal;
        }catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    private Map<Long, Integer> getThunderCurYear() {
        Map<Long, Integer> currentYearVal = new HashMap<>();
        String baseUrl = JsonServiceURL.THUNDER_JSON_SERVICE_URL + "GetThunderData/LS/";
        String endDate = DateHelper.getNow().substring(0, 8) + "000000";
        String beginDate = endDate;
        String stopDate = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1);
        while (true) {
            int cnt = 0;
            endDate = beginDate;
            beginDate = DateHelper.getPostponeDateByDay(endDate, -1);
            if(beginDate.compareTo(stopDate) < 0) break;
            String url = baseUrl + beginDate + "/" + endDate;
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            if(disasterJson != null && disasterJson.get("Data") != null) {
                JSONArray disasterData = (JSONArray) disasterJson.get("Data");
                if (disasterData.size() <= 0) continue;
                boolean isok = false;
                for (Object obj: disasterData) {
                    JSONObject item = (JSONObject) obj;
                    Double lon = (Double) item.get("LON");
                    Double lat = (Double) item.get("LAT");
                    if (lon > AreaHelper.SHANGHAI_LON_MIN &&
                            lon < AreaHelper.SHANGHAI_LON_MAX &&
                            lat > AreaHelper.SHANGHAI_LAT_MIN &&
                            lat < AreaHelper.SHANGHAI_LAT_MAX){
                        isok = true;
                        break;
                    }
                }
                if(isok) cnt++;
            }else continue;
            String month = beginDate.substring(0, 6) + "01000000";
            if(DateHelper.getNow().substring(4, 6).compareTo(month.substring(4, 6)) < 0)
                month = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4) + month.substring(4, 6) + "01000000";
            else
                month = DateHelper.getNow().substring(0, 4) + month.substring(4, 6) + "01000000";
            month = DateHelper.getTimeMillis(month);
            Long monthVal = Long.parseLong(month);
            Integer num = currentYearVal.get(monthVal) == null ? 0 : currentYearVal.get(monthVal);
            if(cnt > 0) num++;
            currentYearVal.put(monthVal, num);
        }
        return currentYearVal;
    }

    private Map<Long, Double> getThunderHisroty(){
        int year = 10;
        Map<Long, Double> weekAvgYearVal = new HashMap<>();
        String baseUrl = JsonServiceURL.THUNDER_JSON_SERVICE_URL + "GetThunderData/LS/";
        String endDate = DateHelper.getNow().substring(0, 8) + "000000";
        String beginDate = endDate;
        String stopDate = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -10);
        while (true) {
            int cnt = 0;
            endDate = beginDate;
            beginDate = DateHelper.getPostponeDateByDay(endDate, -1);
            if(beginDate.compareTo(stopDate) < 0) break;
            String url = baseUrl + beginDate + "/" + endDate;
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            if(disasterJson != null && disasterJson.get("Data") != null) {
                JSONArray disasterData = (JSONArray) disasterJson.get("Data");
                boolean isok = false;
                if (disasterData.size() <= 0) continue;
                for (Object obj: disasterData) {
                    JSONObject item = (JSONObject) obj;
                    Double lat = (Double) item.get("LAT");
                    Double lon = (Double) item.get("LON");
                    if (lon > AreaHelper.SHANGHAI_LON_MIN &&
                            lon < AreaHelper.SHANGHAI_LON_MAX &&
                            lat > AreaHelper.SHANGHAI_LAT_MIN &&
                            lat < AreaHelper.SHANGHAI_LAT_MAX){
                        isok = true;
                        break;
                    }
                }
                if(isok) cnt++;
            }else continue;
            String month = beginDate.substring(0, 6) + "01000000";
            if(DateHelper.getNow().substring(4, 6).compareTo(month.substring(4, 6)) < 0) {
                month = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4) + month.substring(4, 6) + "01000000";
            } else {
                month = DateHelper.getNow().substring(0, 4) + month.substring(4, 6) + "01000000";
            }
            month = DateHelper.getTimeMillis(month);
            Long monthVal = Long.parseLong(month);
            Double num = weekAvgYearVal.get(monthVal) == null ? 0.0 : weekAvgYearVal.get(monthVal);
            if(cnt > 0) num++;
            weekAvgYearVal.put(monthVal, num);
        }
//        for(Map.Entry<Long, Double> entry: weekAvgYearVal.entrySet()){
//            Long monthVal = entry.getKey();
//            weekAvgYearVal.put(monthVal, weekAvgYearVal.get(monthVal) / year);
//        }
        return weekAvgYearVal;
    }

    private void addLocalData(Map<Long, Integer> weekAvgYearVal, String name, String type, String stopDate){
        JSONArray localDataArrray = disPreventDataDAO.findDisPreventDataByName(name).getValue();
        type = "大风".equals(type) ? "wind" : "雷电".equals(type) ? "thunder" : "rain";
        stopDate = DateHelper.getTimeMillis(stopDate);
        for(Object obj: localDataArrray){
            Map<String, String> localData = (Map<String, String>) obj;
            String month = localData.get("FORECASTDATE");
            month = DateHelper.getFormatWarningMonth(month, month.substring(0, 4));
            if (stopDate.compareTo(month) > 0) continue;
            Long monthVal = Long.parseLong(month);
            Integer localCnt = Integer.parseInt(localData.get(type));
            Integer cnt = weekAvgYearVal.get(monthVal) == null ? localCnt : localCnt + weekAvgYearVal.get(monthVal);
            weekAvgYearVal.put(monthVal, cnt);
        }
    }

    @PostConstruct
    public void funcTest() {
//        getDisasterAvg("大风", DisPreventTaskName.FZJZ_WIND_YEAR);
//        getDisasterAvg("暴雨", DisPreventTaskName.FZJZ_RAINFALL_YEAR);
//        getDisasterCurYearByCsvFile("大风", "G:\\work\\crawler\\file_output\\wind");
    }

//    @PostConstruct
    private Map<Long, Double> getHistory(String name, String type) {
        Map<Long, Double> res = new HashMap<>();
        JSONArray localDataArrray = disPreventDataDAO.findDisPreventDataByName(name).getValue();
        type = "大风".equals(type) ? "wind" : "雷电".equals(type) ? "thunder" : "rain";
        for(Object obj: localDataArrray){
            Map<String, String> localData = (Map<String, String>) obj;
            String month = localData.get("month");
            if(DateHelper.getNow().substring(4, 6).compareTo(DateHelper.getMonth(month)) < 0)
                month = DateHelper.getFormatWarningMonth(month, DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4));
            else
                month = DateHelper.getFormatWarningMonth(month, DateHelper.getNow().substring(0, 4));
            Long monthVal = Long.parseLong(month);
            Integer localCnt = Integer.parseInt(localData.get(type));
            Double cnt = res.get(monthVal) == null ? localCnt : localCnt + res.get(monthVal);
            res.put(monthVal, cnt);
        }
        System.out.println(res);
        return res;
    }

    private Map<Long, Integer> getThunderHistory( Map<Long, Integer> res, String stopDate){
        String baseUrl = JsonServiceURL.THUNDER_JSON_SERVICE_URL + "GetThunderData/LS/";
        String endDate = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1);
        endDate = endDate.substring(0, 4) + "0101000000";
        String beginDate = endDate;
        int testCnt = 0;
        while (true) {
            int cnt = 0;
            endDate = beginDate;
            beginDate = DateHelper.getPostponeDateByDay(endDate, -1);
            if(beginDate.compareTo(stopDate) < 0) break;
            String url = baseUrl + beginDate + "/" + endDate;
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            if(disasterJson != null && disasterJson.get("Data") != null) {
                JSONArray disasterData = (JSONArray) disasterJson.get("Data");
                if(disasterData.size() > 0) {
                    System.out.println(disasterData);
                    cnt++;
                }
            }else continue;
            String month;
            if (DateHelper.getNow().substring(4, 6).compareTo(beginDate.substring(4, 6)) < 0) {
                String year = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -1).substring(0, 4);
                month = DateHelper.getTimeMillis(year + beginDate.substring(4, 6) + "01000000");
            }else {
                String year = DateHelper.getNow().substring(0, 4);
                month = DateHelper.getTimeMillis(year + beginDate.substring(4, 6) + "01000000");
            }
            Long monthVal = Long.parseLong(month);
            Integer num = res.get(monthVal) == null ? 0 : res.get(monthVal);
            if(cnt > 0) num++;
            if(cnt > 0) testCnt++;
            System.out.println(testCnt);
            res.put(monthVal, num);
        }
        return res;
    }

    public int getPastTenYearWarning() {
        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";
        int total = 0, count = 0;
        for (int i = 0; i < 10; ++i) {
            String endDate = DateHelper.getPostponeDateByYear(DateHelper.getNow(), -i).substring(0, 4) + "1231235959";
            String beginDate = endDate.substring(0, 4) + "0101000000";
            String url = baseUrl + beginDate + "/" + endDate;
            JSONObject disasterJson = HttpHelper.getDataByURL(url);
            JSONArray disasterData = (JSONArray) disasterJson.get("Data");
            int amount = 0;
            for (Object obj: disasterData) {
                JSONObject curWarning = (JSONObject) obj;

                // 如果不是风雨雷，就跳过
                if (!WarningHelper.TYPE_WIND.equals(curWarning.get("TYPE"))
                        && !WarningHelper.TYPE_RAIN.equals(curWarning.get("TYPE"))
                        && !WarningHelper.TYPE_THUNDER.equals(curWarning.get("TYPE")))
                    continue;
                // 如果不是红橙黄蓝，就跳过
                if (!WarningHelper.LEVEL_BLUE.equals(curWarning.get("LEVEL"))
                        && !WarningHelper.LEVEL_ORANGE.equals(curWarning.get("LEVEL"))
                        && !WarningHelper.LEVEL_RED.equals(curWarning.get("LEVEL"))
                        && !WarningHelper.LEVEL_YELLOW.equals(curWarning.get("LEVEL")))
                    continue;

                ++amount;
            }
            if (amount != 0) {
                total += amount;
            }
            ++count;
        }

        if (count == 0) return 0;
        return total / count;
    }
}