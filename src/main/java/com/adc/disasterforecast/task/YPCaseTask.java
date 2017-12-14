//package com.adc.disasterforecast.task;
//
//import com.adc.disasterforecast.dao.YPCaseDataDAO;
//import com.adc.disasterforecast.entity.YPCaseDataEntity;
//import com.adc.disasterforecast.global.JsonServiceURL;
//import com.adc.disasterforecast.global.YPCaseTaskName;
//import com.adc.disasterforecast.global.YPRegionInfo;
//import com.adc.disasterforecast.tools.DateHelper;
//import com.adc.disasterforecast.tools.HttpHelper;
//import com.adc.disasterforecast.tools.CsvHelper;
//import com.adc.disasterforecast.tools.WarningHelper;
//import com.mongodb.util.JSON;
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.boot.context.event.ApplicationReadyEvent;
//import org.springframework.context.event.EventListener;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import java.util.*;
//
//@Component
//public class YPCaseTask {
//    // logger for FeiteTask
//    private static final Logger logger = LoggerFactory.getLogger(YPCaseTask.class);
//
//    // dao Autowired
//    @Autowired
//    private YPCaseDataDAO ypCaseDataDAO;
//
//    @EventListener(ApplicationReadyEvent.class)
//    public void countSurvey() {
//        String baseUrl = JsonServiceURL.VERIFY_USER_URL + "GetCommunityListByDistrict/";
//
//        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_SURVEY));
//
//        String url = baseUrl + YPRegionInfo.YP_DISTRICT;
//        JSONObject streetsJson = HttpHelper.getDataByURL(url);
//
//        int streetNum = ((JSONArray) streetsJson.get("Data")).size() - 1;
//
//        JSONArray surveyValue = new JSONArray();
//
//        JSONObject ypSurvey = new JSONObject();
//        ypSurvey.put("jiedao", String.valueOf(streetNum));
//        ypSurvey.put("mianji", YPRegionInfo.YP_ACREAGE);
//        ypSurvey.put("renkou", YPRegionInfo.YP_POPULATION);
//
//        surveyValue.add(ypSurvey);
//
//        YPCaseDataEntity survey = new YPCaseDataEntity();
//        survey.setName(YPCaseTaskName.YPCASE_SURVEY);
//        survey.setValue(surveyValue);
//
//        ypCaseDataDAO.updateYPCaseDataByName(survey);
//    }
//
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
//    public void countHistoryDisaster() throws InterruptedException {
//        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";
//
//        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_HISTROY_DISASTER));
//
//        JSONArray ypHistoryDisasterValue = new JSONArray();
//
//        // 获取开始年份
//        int years = Calendar.getInstance().get(Calendar.YEAR) - 2011;
//
//        for (int i = 0; i < 12; i++) {
//            int monthDisasterSum = 0;
//
//            for (int j = 0; j < years; j++) {
//                String beginTime = DateHelper.getPostponeDateByMonth(2012 + j, 1, 1, 0, 0, 0, i);
//                String endTime = DateHelper.getPostponeDateByMonth(2012 + j, 1, 1, 0, 0, 0, i + 1);
//
//                String url = baseUrl + beginTime + "/" + endTime;
//                JSONObject historyDisasterJson = HttpHelper.getDataByURL(url);
//                JSONArray historyDisasters = (JSONArray) historyDisasterJson.get("Data");
//
//                monthDisasterSum += getYPMonthlyHistoryDisaster(historyDisasters);
//            }
//
//            int mouthAvg = monthDisasterSum / years;
//
//            JSONObject ypMonthlyAvgHistoryDisaster = new JSONObject();
//            ypMonthlyAvgHistoryDisaster.put("month", i + 1);
//            ypMonthlyAvgHistoryDisaster.put("value", mouthAvg);
//
//            ypHistoryDisasterValue.add(ypMonthlyAvgHistoryDisaster);
//        }
//
//        YPCaseDataEntity historyDisaster = new YPCaseDataEntity();
//        historyDisaster.setName(YPCaseTaskName.YPCASE_HISTROY_DISASTER);
//        historyDisaster.setValue(ypHistoryDisasterValue);
//
//        ypCaseDataDAO.updateYPCaseDataByName(historyDisaster);
//    }
//
//    @EventListener(ApplicationReadyEvent.class)
//    public void countNotice() {
//        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_NOTICE));
//
//        JSONArray noticeValue = new JSONArray();
//
//        JSONObject ypNotice = new JSONObject();
//        ypNotice.put("title", YPRegionInfo.YP_NOTICE_TITLE);
//        ypNotice.put("content", YPRegionInfo.YP_NOTICE_CONTENT);
//
//        noticeValue.add(ypNotice);
//
//        YPCaseDataEntity notice = new YPCaseDataEntity();
//        notice.setName(YPCaseTaskName.YPCASE_NOTICE);
//        notice.setValue(noticeValue);
//
//        ypCaseDataDAO.updateYPCaseDataByName(notice);
//    }
//
//    private int getYPMonthlyHistoryDisaster(JSONArray historyDisasters) {
//        int disasterNum = 0;
//        int ypDistrict = Integer.valueOf(YPRegionInfo.YP_DISTRICT);
//
//        for (Object obj : historyDisasters) {
//            JSONObject annualHistoryDisaster = (JSONObject) obj;
//            if (((Number) annualHistoryDisaster.get("DISTRICT")).intValue() == ypDistrict) {
//                disasterNum ++;
//            }
//        }
//
//        return disasterNum;
//    }
//    /**
//    * @description:
//    * @author: zhichengliu
//    * @date: 17/11/26
//    **/
//    private JSONArray getRainFall(String [] stationName) {
//        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";
//        logger.info(String.format("began task: %s", YPCaseTaskName.YPCASE_RAIN_TIME));
//
//        JSONArray rainfallVal = new JSONArray();
//        for (int i = 0; i < YPRegionInfo.Hours; i++){
//            String beginDate = DateHelper.getPostponeDateByHour(2015, 6, 16, 12, 0, 0, i);
//            String endDate = DateHelper.getPostponeDateByHour(2015, 6, 16, 13, 0, 0, i);
//            String url = baseUrl + beginDate + "/" + endDate + "/1";
//            JSONArray rainfallData = (JSONArray) HttpHelper.getDataByURL(url).get("Data");
//
//            Double rainfallSum = 0.0;
//            HashMap<String, Double> hs = new HashMap<>();
//            for (Object obj: rainfallData){
//                JSONObject rainfall = (JSONObject) obj;
//
//                for (String s: stationName) {
//                    if (s.equals(rainfall.get("STATIONNAME"))) {
//                        if (hs.get(s) == null) hs.put(s, Double.parseDouble((String) rainfall.get("RAINHOUR")));
//                        else hs.put(s, Math.max(Double.parseDouble((String) rainfall.get("RAINHOUR")), hs.get(s)));
//                    }
//                }
//            }
//            Iterator iter = hs.entrySet().iterator();
//            while (iter.hasNext()) {
//                Map.Entry entry = (Map.Entry) iter.next();
//                rainfallSum += (Double) entry.getValue();
////                System.out.println(entry.getKey() + String.valueOf(entry.getValue()));
//            }
//            JSONObject rainfallAvg = new JSONObject();
//            rainfallAvg.put("time", beginDate);
//            rainfallAvg.put("value", rainfallSum / stationName.length);
//            rainfallVal.add(rainfallAvg);
//        }
//        return rainfallVal;
//    }
//    /**
//    * @description:
//    * @author: zhichengliu
//    * @date: 17/11/26
//    **/
//    private JSONArray getSeeper(String [] stationName) {
//        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterStationDataByDatetime_Geliku/";
//        logger.info(String.format("began task: %s", YPCaseTaskName.YPCASE_SEEPER_TIME));
//
//        JSONArray seeperVal = new JSONArray();
//        for (int i = 0; i < YPRegionInfo.Hours; i++) {
//            String beginDate = DateHelper.getPostponeDateByHour(2015, 6, 16, 12, 0, 0, i);
//            String endDate = DateHelper.getPostponeDateByHour(2015, 6, 16, 13, 0, 0, i);
//            String url = baseUrl + beginDate + "/" + endDate;
//            String DataUrl = (String) HttpHelper.getDataByURL(url).get("Data");
//            JSONArray seeperData = CsvHelper.getDataByURL(DataUrl);
//            Double seeperSum = 0.0;
//            HashMap<String, Double> hs = new HashMap<>();
//            for (Object obj : seeperData) {
//                JSONObject seeper = (JSONObject) obj;
//
//                for (String s : stationName) {
//                    if (s.equals(seeper.get("STATIONNAME"))) {
//                        if (hs.get(s) == null) hs.put(s, Double.parseDouble((String) seeper.get("WATERDEPTH")));
//                        else hs.put(s, Math.max(Double.parseDouble((String) seeper.get("WATERDEPTH")), hs.get(s)));
//                    }
//                }
//            }
//            Iterator iter = hs.entrySet().iterator();
//            while (iter.hasNext()) {
//                Map.Entry entry = (Map.Entry) iter.next();
//                seeperSum += (Double) entry.getValue();
//            }
//            JSONObject seeperAvg = new JSONObject();
//            seeperAvg.put("time", beginDate);
//            seeperAvg.put("value", seeperSum / stationName.length);
//            seeperVal.add(seeperAvg);
//        }
//        return seeperVal;
//    }
//
//    /**
//    * @Description 预警服务过程（使用导出的数据 静态）
//    * @Author lilin
//    * @Create 2017/11/28 17:29
//    **/
//    @EventListener(ApplicationReadyEvent.class)
//    public void getWarningService() {
//        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_WARNING_SERVICE));
//
//        JSONObject obj = WarningHelper.getWarningServiceContent();
//        JSONArray resultArray = new JSONArray();
//
//        JSONArray warnings = (JSONArray) obj.get("Data");
//        for (int i = 0; i < warnings.size(); i++) {
//            JSONObject warning = (JSONObject) warnings.get(i);
//            JSONObject resultObject = new JSONObject();
//            String id = (String) warning.get("id");
//            String time = (String) warning.get("time");
//            String desc = (String) warning.get("desc");
//            resultObject.put("id", id);
//            resultObject.put("time", Long.parseLong(DateHelper.getTimeMillis(time)));
//            resultObject.put("desc", desc);
//            resultArray.add(resultObject);
//        }
//        YPCaseDataEntity warningService = new YPCaseDataEntity();
//        warningService.setName(YPCaseTaskName.YPCASE_WARNING_SERVICE);
//        warningService.setValue(resultArray);
//        ypCaseDataDAO.updateYPCaseDataByName(warningService);
//    }
//
//    /**
//    * @Description 统计分时段暴雨积水情况（新江湾城1#没有数据）
//    * @Author lilin
//    * @Create 2017/11/28 19:07
//    **/
//    public void countRainAndSeeperByAlarmId(String beginDate, String endDate, String alarmId) {
//        String url = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/" +
//                DateHelper.getNowHour(endDate) + "/" + DateHelper.getNextHour(endDate) + "/1";
//        JSONObject obj = HttpHelper.getDataByURL(url);
//
//        String seeperUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetWaterStationData_Geliku/" + endDate;
//        JSONObject seeperObj = HttpHelper.getDataByURL(seeperUrl);
//
//        JSONArray resultArray = new JSONArray();
//        JSONObject rainObject = new JSONObject();
//        JSONObject seeperObject = new JSONObject();
//
//        // 暴雨情况
//        JSONArray rainValueArray = new JSONArray();
//        String rainValue = "";
//
//        JSONArray autoStationDataArray = (JSONArray) obj.get("Data");
//        for (int i = 0; i < autoStationDataArray.size(); i++) {
//            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(i);
//            String stationName = (String) autoStationData.get("STATIONNAME");
//            if ("新江湾城街道".equals(stationName)) {
//                rainValue = (String) autoStationData.get("RAINHOUR");
//                break;
//            }
//        }
//        JSONObject rainValueObject = new JSONObject();
//        rainValueObject.put("site", "新江湾城街道");
//        rainValueObject.put("value", Double.parseDouble(rainValue));
//
//        rainValueArray.add(rainValueObject);
//
//        rainObject.put("type", "baoyu");
//        rainObject.put("value", rainValueArray);
//
//        // 积水情况
//        JSONArray seeperValueArray = new JSONArray();
//        // 政立路545弄
//        String seeperValue_ZLL = "";
//        // 时代花园
//        String seeperValue_SDHY = "";
//
//        JSONArray waterStationDataArray = (JSONArray) seeperObj.get("Data");
//        for (int i = 0; i < waterStationDataArray.size(); i++) {
//            JSONObject waterStationData = (JSONObject) waterStationDataArray.get(i);
//            String stationName = (String) waterStationData.get("STATIONNAME");
//            if ("政立路545弄".equals(stationName)) {
//                seeperValue_ZLL = (Double) waterStationData.get("WATERDEPTH") * 100 + "";
//            }
//            if ("时代花园".equals(stationName)) {
//                seeperValue_SDHY = (Double) waterStationData.get("WATERDEPTH") * 100 + "";
//            }
//            if (!seeperValue_ZLL.isEmpty() && !seeperValue_SDHY.isEmpty()) {
//                break;
//            }
//        }
//        JSONObject seeperValueObject_ZLL = new JSONObject();
//        seeperValueObject_ZLL.put("site", "政立路545弄");
//        seeperValueObject_ZLL.put("value", Double.parseDouble(seeperValue_ZLL));
//        JSONObject seeperValueObject_SDHY = new JSONObject();
//        seeperValueObject_SDHY.put("site", "时代花园");
//        seeperValueObject_SDHY.put("value", Double.parseDouble(seeperValue_SDHY));
//
//        seeperValueArray.add(seeperValueObject_ZLL);
//        seeperValueArray.add(seeperValueObject_SDHY);
//
//        seeperObject.put("type", "jishui");
//        seeperObject.put("value", seeperValueArray);
//
//        resultArray.add(rainObject);
//        resultArray.add(seeperObject);
//
//        YPCaseDataEntity rainSeeperData = new YPCaseDataEntity();
//        rainSeeperData.setAlarmId(alarmId);
//        rainSeeperData.setName(YPCaseTaskName.YPCASE_RAIN_SEEPER);
//        rainSeeperData.setValue(resultArray);
//        ypCaseDataDAO.updateYPCaseDataByNameAndAlarmId(rainSeeperData);
//    }
//
//    /**
//    * @Description 统计暴雨积水情况
//    * @Author lilin
//    * @Create 2017/11/28 19:58
//    **/
//    @EventListener(ApplicationReadyEvent.class)
//    public void countRainAndSeeper() {
//        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_RAIN_SEEPER));
//
//        JSONArray alarms = ypCaseDataDAO.findYPCaseDataByName("YPCASE_ALARM_STAGE").getValue();
//
//        for (Object obj : alarms) {
//            Map<String, String> alarm = (Map<String, String>) obj;
//            String beginDate = alarm.get("beginDate");
//            String endDate = alarm.get("endDate");
//            String alarmId = alarm.get("alarmId");
//            countRainAndSeeperByAlarmId(beginDate, endDate, alarmId);
//        }
//    }
//    /**
//    * @description:
//    * @author: zhichengliu
//    * @date: 17/11/29
//    **/
//    private void countRainAndSeeperByTime(String [] rainfallStationName, String [] seeperStaionName, String rainfallTaskName, String seeperTaskName) {
//        String beginDate = YPRegionInfo.BEGINDATE;
//        String endDate = YPRegionInfo.ENDDATE;
//        JSONArray allRainfall = getRainFall(rainfallStationName);
//        JSONArray allSeeper = getSeeper(seeperStaionName);
//        JSONArray rainfallVal = new JSONArray();
//
//        for (Object o: allRainfall){
//            JSONObject rainfall = (JSONObject) o;
//            JSONObject formatRainfall = new JSONObject();
//            if (beginDate.compareTo((String)rainfall.get("time")) > 0) continue;
//            if (endDate.compareTo((String)rainfall.get("time")) <= 0) continue;
//            formatRainfall.put("value", rainfall.get("value"));
//            formatRainfall.put("time", Long.parseLong(DateHelper.getTimeMillis((String)rainfall.get("time"))));
//            rainfallVal.add(formatRainfall);
//        }
//        YPCaseDataEntity rainfallData = new YPCaseDataEntity();
//        rainfallData.setName(rainfallTaskName);
//        rainfallData.setValue(rainfallVal);
//        ypCaseDataDAO.updateYPCaseDataByName(rainfallData);
//
//        JSONArray seeperVal = new JSONArray();
//        for (Object o: allSeeper){
//            JSONObject seeper = (JSONObject) o;
//            JSONObject formatSeeper = new JSONObject();
//            if (endDate.compareTo((String)seeper.get("time")) <= 0) continue;
//            if (beginDate.compareTo((String)seeper.get("time")) > 0) continue;
//            formatSeeper.put("value", (Double)seeper.get("value") * 100);
//            formatSeeper.put("time", Long.parseLong(DateHelper.getTimeMillis((String)seeper.get("time"))));
//            seeperVal.add(formatSeeper);
//        }
//        YPCaseDataEntity seeperData = new YPCaseDataEntity();
//        seeperData.setName(seeperTaskName);
//        seeperData.setValue(seeperVal);
//        ypCaseDataDAO.updateYPCaseDataByName(seeperData);
//    }
//
//    @EventListener(ApplicationReadyEvent.class)
//    public void countRainAndSeeperByStation(){
//        countRainAndSeeperByTime(YPRegionInfo.XINJIANGWAN_RAIN_STATIONNAME, YPRegionInfo.XINJIANGWAN_SEEPER_STATIONNAME,
//                YPCaseTaskName.YPCASE_RAIN_TIME, YPCaseTaskName.YPCASE_SEEPER_TIME);
//        countRainAndSeeperByTime(YPRegionInfo.YANGPU_RAIN_STATIONNAME, YPRegionInfo.YANGPU_SEEPER_STATIONNAME,
//                YPCaseTaskName.YPCASE_RAIN_WEEKAVG, YPCaseTaskName.YPCASE_SEEPER_WEEKAVG);
//    }
//}
