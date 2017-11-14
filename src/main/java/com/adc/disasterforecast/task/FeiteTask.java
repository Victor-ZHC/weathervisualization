package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import com.adc.disasterforecast.global.FeiteTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.HttpHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class FeiteTask {
    // logger for FeiteTask
    private static final Logger logger = LoggerFactory.getLogger(FeiteTask.class);

    // region info
    private static final String yangpuDistrict = "14";
    private static final String yangpuArea = "杨浦区";
    private static final Double yangpuPopulation = 131.32;
    private static final Double yangpuAcreage = 60.61;

    private static final String chongmingDistrict = "10";
    private static final String chongmingArea = "崇明区";
    private static final Double chongmingPopulation = 67.26;
    private static final Double chongmingAcreage = 1411.0;

    // dao Autowired
    @Autowired
    private DataDAO dataDAO;

    @Scheduled(cron = "00 * * * * *")
    public void countRegionDiff() throws InterruptedException {
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_DIFF));

        JSONObject yangpu = new JSONObject();
        yangpu.put("area", yangpuArea);
        yangpu.put("population", yangpuPopulation);
        yangpu.put("acreage", yangpuAcreage);

        JSONObject chongming = new JSONObject();
        chongming.put("area", chongmingArea);
        chongming.put("population", chongmingPopulation);
        chongming.put("acreage", chongmingAcreage);

        JSONArray diffValue = new JSONArray();
        diffValue.add(yangpu);
        diffValue.add(chongming);

        DataEntity diff = new DataEntity();
        diff.setName(FeiteTaskName.FEITE_REGION_DIFF);
        diff.setValue(diffValue);

        dataDAO.updateExample(diff);
    }

//    @Scheduled(cron = "20 * * * * *")
//    public void countRegionRainfallDiff() throws InterruptedException {
//        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterHistory/20131006000000/20131009000000";
//
//        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_DIFF));
//
//        JSONObject obj = HttpHelper.getDataByURL(url);
//
//        JSONArray disasterData = (JSONArray) obj.get("Data");
//
//
////        dataDAO.updateExample(dataEntity);
//    }

    @Scheduled(cron = "30 * * * * *")
    public void countRegionDisasterDiff() throws InterruptedException {
        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterHistory/20131006000000/20131009000000";

        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_DIFF));

        JSONObject obj = HttpHelper.getDataByURL(url);

        // 统计两地受灾数
        List<JSONObject> yangpuDisasters = new ArrayList<>();
        List<JSONObject> chongmingDisasters = new ArrayList<>();

        JSONArray disasters = (JSONArray) obj.get("Data");
        for (Object disaster : disasters) {
            JSONObject disasterData = (JSONObject) disaster;
            String disasterDistrict = (String) disasterData.get("Disaster_District");

            if (yangpuDistrict.equals(disasterDistrict)) {
                yangpuDisasters.add(disasterData);
            } else if (chongmingDistrict.equals(disasterDistrict)) {
                chongmingDisasters.add(disasterData);
            }
        }

        JSONObject yangpuNumDiff = new JSONObject();
        yangpuNumDiff.put("area", yangpuArea);
        yangpuNumDiff.put("value", yangpuDisasters.size());

        JSONObject chongmingNumDiff = new JSONObject();
        chongmingNumDiff.put("area", chongmingArea);
        chongmingNumDiff.put("value", chongmingDisasters.size());

        JSONArray numDiffValue = new JSONArray();
        numDiffValue.add(yangpuNumDiff);
        numDiffValue.add(chongmingNumDiff);

        DataEntity numDiff = new DataEntity();
        numDiff.setName(FeiteTaskName.FEITE_REGION_DISASTER_NUM_DIFF);
        numDiff.setValue(numDiffValue);

        dataDAO.updateExample(numDiff);

        // 统计两地受灾密度
        JSONObject yangpuDensityDiff = new JSONObject();
        yangpuDensityDiff.put("area", yangpuArea);
        yangpuDensityDiff.put("value", ((double) yangpuDisasters.size()) / yangpuAcreage);

        JSONObject chongmingDensityDiff = new JSONObject();
        chongmingDensityDiff.put("area", chongmingArea);
        chongmingDensityDiff.put("value", ((double) chongmingDisasters.size()) / chongmingAcreage);

        JSONArray densityDiffValue = new JSONArray();
        densityDiffValue.add(yangpuDensityDiff);
        densityDiffValue.add(chongmingDensityDiff);

        DataEntity densityDiff = new DataEntity();
        densityDiff.setName(FeiteTaskName.FEITE_REGION_DISASTER_DENSITY_DIFF);
        densityDiff.setValue(densityDiffValue);

        dataDAO.updateExample(densityDiff);

        // 统计两地受灾种类数
        JSONObject yangpuDisasterType = getAreaDisasterType(yangpuArea, yangpuDisasters);
        JSONObject chongmingDisasterType = getAreaDisasterType(chongmingArea, chongmingDisasters);

        JSONArray typeDiffValue = new JSONArray();
        typeDiffValue.add(yangpuDisasterType);
        typeDiffValue.add(chongmingDisasterType);

        DataEntity typeDiff = new DataEntity();
        typeDiff.setName(FeiteTaskName.FEITE_REGION_DISASTER_TYPE_DIFF);
        typeDiff.setValue(typeDiffValue);

        dataDAO.updateExample(typeDiff);
    }

//    @Scheduled(cron = "* * * * * *")
//    public void countRegionSeeperDiff() throws InterruptedException {
//        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterHistory/20131006000000/20131009000000";
//
//        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_DIFF));
//
//        JSONObject obj = HttpHelper.getDataByURL(url);
//
//        JSONArray disasterData = (JSONArray) obj.get("Data");
//
//
////        dataDAO.updateExample(dataEntity);
//    }

    private JSONObject getAreaDisasterType(String area, List<JSONObject> disasters) {
        int[] disasterType = new int[7];
        String[] disasterTypeName = {"树倒", "河水上涨", "农田作物", "房屋进水", "小区进水", "高空坠物", "其他"};

        for (JSONObject disaster : disasters) {
            String code = (String) disaster.get("Disaster_Code");
            if ("2".equals(code)) {
                disasterType[0] += 1;
            } else {
                String description = (String) disaster.get("Disaster_Description");
                if (description.contains("河水")) {
                    disasterType[1] += 1;
                } else if (description.contains("田地") || description.contains("农田")) {
                    disasterType[2] += 1;
                } else if (description.contains("房屋") || description.contains("家")) {
                    disasterType[3] += 1;
                } else if (description.contains("小区")) {
                    disasterType[4] += 1;
                } else if (description.contains("坠")) {
                    disasterType[5] += 1;
                } else {
                    disasterType[6] += 1;
                }
            }
        }

        JSONArray disasterTypeList = new JSONArray();

        for (int i = 0; i < disasterType.length; i++) {
            JSONObject obj = new JSONObject();
            obj.put("type", disasterTypeName[i]);
            obj.put("value", disasterType[i]);
            disasterTypeList.add(obj);
        }

        JSONObject areaDisasterType = new JSONObject();
        areaDisasterType.put("area", area);
        areaDisasterType.put("value", disasterTypeList);

        return areaDisasterType;
    }
}
