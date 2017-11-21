package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.YPCaseTaskName;
import com.adc.disasterforecast.global.YPRegionInfo;
import com.adc.disasterforecast.tools.HttpHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Calendar;

@Component
public class YPCaseTask {
    // logger for FeiteTask
    private static final Logger logger = LoggerFactory.getLogger(YPCaseTask.class);

    // dao Autowired
    @Autowired
    private DataDAO dataDAO;

    @Scheduled(cron = "00 * * * * *")
    public void countSurvey() throws InterruptedException {
        String baseUrl = JsonServiceURL.VERIFY_USER_URL + "GetCommunityListByDistrict/";

        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_SURVEY));

        String url = baseUrl + YPRegionInfo.YP_DISTRICT;
        JSONObject streetsJson = HttpHelper.getDataByURL(url);

        int streetNum = ((JSONArray) streetsJson.get("Data")).size() - 1;

        JSONArray surveyValue = new JSONArray();

        JSONObject ypSurvey = new JSONObject();
        ypSurvey.put("jiedao", String.valueOf(streetNum));
        ypSurvey.put("mianji", YPRegionInfo.YP_ACREAGE);
        ypSurvey.put("renkou", YPRegionInfo.YP_POPULATION);

        surveyValue.add(ypSurvey);

        DataEntity survey = new DataEntity();
        survey.setName(YPCaseTaskName.YPCASE_SURVEY);
        survey.setValue(surveyValue);

        dataDAO.updateExample(survey);
    }

    @Scheduled(cron = "20 * * * * *")
    public void countHistoryDisaster() throws InterruptedException {
        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_HISTROY_DISASTER));

        JSONArray ypHistoryDisasterValue = new JSONArray();

        // 获取开始年份
        int baseYear = Calendar.getInstance().get(Calendar.YEAR) - 5;
        for (int i = 0; i < 6; i++) {
            String beginTime = (baseYear + i) + "0101000000";
            String endTime = (baseYear + i + 1) + "0101000000";

            String url = baseUrl + beginTime + "/" + endTime;
            JSONObject historyDisasterJson = HttpHelper.getDataByURL(url);
            JSONArray historyDisasters = (JSONArray) historyDisasterJson.get("Data");

            JSONObject ypAnnualHistoryDisaster = getYPAnnualHistoryDisaster(historyDisasters, baseYear + i);

            ypHistoryDisasterValue.add(ypAnnualHistoryDisaster);
        }

        DataEntity historyDisaster = new DataEntity();
        historyDisaster.setName(YPCaseTaskName.YPCASE_HISTROY_DISASTER);
        historyDisaster.setValue(ypHistoryDisasterValue);

        dataDAO.updateExample(historyDisaster);
    }

    @Scheduled(cron = "50 * * * * *")
    public void countNotice() throws InterruptedException {
        logger.info(String.format("began task：%s", YPCaseTaskName.YPCASE_NOTICE));

        JSONArray noticeValue = new JSONArray();

        JSONObject ypNotice = new JSONObject();
        ypNotice.put("title", YPRegionInfo.YP_NOTICE_TITLE);
        ypNotice.put("content", YPRegionInfo.YP_NOTICE_CONTENT);

        noticeValue.add(ypNotice);

        DataEntity notice = new DataEntity();
        notice.setName(YPCaseTaskName.YPCASE_NOTICE);
        notice.setValue(noticeValue);

        dataDAO.updateExample(notice);
    }

    private JSONObject getYPAnnualHistoryDisaster(JSONArray historyDisasters, int year) {
        int disasterNum = 0;
        int ypDistrict = Integer.valueOf(YPRegionInfo.YP_DISTRICT);
        for (Object obj : historyDisasters) {
            JSONObject annualHistoryDisaster = (JSONObject) obj;
            if (((Number) annualHistoryDisaster.get("DISTRICT")).intValue() == ypDistrict) {
                disasterNum ++;
            }
        }

        JSONObject historyDisasterResult = new JSONObject();
        historyDisasterResult.put("year", year);
        historyDisasterResult.put("value", disasterNum);

        return historyDisasterResult;
    }
}
