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
//import org.json.simple.JSONArray;
//import org.json.simple.JSONObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
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
//    @PostConstruct
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
//    @PostConstruct
//    @Scheduled(cron = "0 0 0 * * *")
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
//    @PostConstruct
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
//    @PostConstruct
//    public void getRainFall() {
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
//                for (String s: YPRegionInfo.YANGPU_RAIN_STATIONNAME) {
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
//            rainfallAvg.put("time", DateHelper.getTimeMillis(endDate));
//            rainfallAvg.put("value", String.valueOf(rainfallSum / YPRegionInfo.YANGPU_RAIN_STATIONNAME.length));
//            rainfallVal.add(rainfallAvg);
//        }
//        YPCaseDataEntity rainfall = new YPCaseDataEntity();
//        rainfall.setName(YPCaseTaskName.YPCASE_RAIN_TIME);
//        rainfall.setValue(rainfallVal);
//        ypCaseDataDAO.updateYPCaseDataByName(rainfall);
//    }
//    /**
//    * @description:
//    * @author: zhichengliu
//    * @date: 17/11/26
//    **/
//    @PostConstruct
//    public void getSeeper() {
//        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetWaterStationDataByDatetime_Geliku/";
//        logger.info(String.format("began task: %s", YPCaseTaskName.YPCASE_SEEPER_TIME));
//
//        JSONArray seeperVal = new JSONArray();
//        for (int i = 0; i < YPRegionInfo.Hours; i++){
//            String beginDate = DateHelper.getPostponeDateByHour(2015, 6, 16, 12, 0, 0, i);
//            String endDate = DateHelper.getPostponeDateByHour(2015, 6, 16, 13, 0, 0, i);
//            String url = baseUrl + beginDate + "/" + endDate;
//            String DataUrl = (String)HttpHelper.getDataByURL(url).get("Data");
//            JSONArray seeperData = CsvHelper.getDataByURL(DataUrl);
//            Double seeperSum = 0.0;
//            HashMap<String, Double> hs = new HashMap<>();
//            for (Object obj: seeperData){
//                JSONObject seeper = (JSONObject) obj;
//
//                for (String s: YPRegionInfo.YANGPU_SEEPER_STATIONNAME) {
//                    if (s.equals(seeper.get("STATIONNAME"))){
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
//            seeperAvg.put("time", DateHelper.getTimeMillis(endDate));
//            seeperAvg.put("value", String.valueOf(seeperSum / YPRegionInfo.YANGPU_SEEPER_STATIONNAME.length));
////            System.out.println(seeperAvg);
//            seeperVal.add(seeperAvg);
//        }
//        YPCaseDataEntity seeper = new YPCaseDataEntity();
//        seeper.setName(YPCaseTaskName.YPCASE_SEEPER_TIME);
//        seeper.setValue(seeperVal);
//        ypCaseDataDAO.updateYPCaseDataByName(seeper);
//
//    }
//}
