package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import com.adc.disasterforecast.global.FeiteRegionInfo;
import com.adc.disasterforecast.global.FeiteTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.DateHelper;
import com.adc.disasterforecast.tools.HttpHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class FeiteTask {
    // logger for FeiteTask
    private static final Logger logger = LoggerFactory.getLogger(FeiteTask.class);

    // dao Autowired
    @Autowired
    private DataDAO dataDAO;

    @Scheduled(cron = "00 * * * * *")
    public void countRegionDiff() throws InterruptedException {
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_DIFF));

        JSONObject yangpu = new JSONObject();
        yangpu.put("area", FeiteRegionInfo.yangpuArea);
        yangpu.put("population", FeiteRegionInfo.yangpuPopulation);
        yangpu.put("acreage", FeiteRegionInfo.yangpuAcreage);

        JSONObject chongming = new JSONObject();
        chongming.put("area", FeiteRegionInfo.chongmingArea);
        chongming.put("population", FeiteRegionInfo.chongmingPopulation);
        chongming.put("acreage", FeiteRegionInfo.chongmingAcreage);

        JSONArray diffValue = new JSONArray();
        diffValue.add(yangpu);
        diffValue.add(chongming);

        DataEntity diff = new DataEntity();
        diff.setName(FeiteTaskName.FEITE_REGION_DIFF);
        diff.setValue(diffValue);

        dataDAO.updateExample(diff);
    }

    @Scheduled(cron = "20 * * * * *")
    public void countRegionRainfallDiff() throws InterruptedException {
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";

        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_RAINFALL_DIFF));

        JSONArray yangpuRainfalls = new JSONArray();
        JSONArray chongmingRainfalls = new JSONArray();

        for (int i = 0; i < 24; i++) {
            String date = DateHelper.getPostponeDateByHour(2013, 10, 7, 13, 0, 0, i);
            String url = baseUrl + date + "/" + date + "/1";
            JSONObject rainfallJson = HttpHelper.getDataByURL(url);
            JSONArray rainfallData = (JSONArray) rainfallJson.get("Data");

            for (Object obj : rainfallData) {
                JSONObject rainfall = (JSONObject) obj;

                if (FeiteRegionInfo.chongmingStationName.equals(rainfall.get("STATIONNAME"))) {
                    addRainfall(chongmingRainfalls, rainfall, i);
                } else if (FeiteRegionInfo.yangpuStationName.equals(rainfall.get("STATIONNAME"))) {
                    addRainfall(yangpuRainfalls, rainfall, i);
                }
            }
        }

        JSONArray rainfallValue = new JSONArray();

        JSONObject chongmingRainfall = new JSONObject();
        chongmingRainfall.put("area", FeiteRegionInfo.chongmingArea);
        chongmingRainfall.put("value", chongmingRainfalls);

        JSONObject yangpuRainfall = new JSONObject();
        yangpuRainfall.put("area", FeiteRegionInfo.yangpuArea);
        yangpuRainfall.put("value", yangpuRainfalls);

        rainfallValue.add(chongmingRainfall);
        rainfallValue.add(yangpuRainfall);

        DataEntity rainfall = new DataEntity();
        rainfall.setName(FeiteTaskName.FEITE_REGION_RAINFALL_DIFF);
        rainfall.setValue(rainfallValue);

        dataDAO.updateExample(rainfall);
    }

    @Scheduled(cron = "40 * * * * *")
    public void countRegionDisasterDiff() throws InterruptedException {
        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetDisasterHistory/20131006200000/20131008120000";

        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_REGION_DIFF));

        JSONObject obj = HttpHelper.getDataByURL(url);

        // 统计两地受灾数
        List<JSONObject> yangpuDisasters = new ArrayList<>();
        List<JSONObject> chongmingDisasters = new ArrayList<>();

        JSONArray disasters = (JSONArray) obj.get("Data");
        for (Object disaster : disasters) {
            JSONObject disasterData = (JSONObject) disaster;
            String disasterDistrict = (String) disasterData.get("Disaster_District");

            if (FeiteRegionInfo.yangpuDistrict.equals(disasterDistrict)) {
                yangpuDisasters.add(disasterData);
            } else if (FeiteRegionInfo.chongmingDistrict.equals(disasterDistrict)) {
                chongmingDisasters.add(disasterData);
            }
        }

        JSONObject yangpuNumDiff = new JSONObject();
        yangpuNumDiff.put("area", FeiteRegionInfo.yangpuArea);
        yangpuNumDiff.put("value", yangpuDisasters.size());

        JSONObject chongmingNumDiff = new JSONObject();
        chongmingNumDiff.put("area", FeiteRegionInfo.chongmingArea);
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
        yangpuDensityDiff.put("area", FeiteRegionInfo.yangpuArea);
        yangpuDensityDiff.put("value", ((double) yangpuDisasters.size()) / FeiteRegionInfo.yangpuAcreage);

        JSONObject chongmingDensityDiff = new JSONObject();
        chongmingDensityDiff.put("area", FeiteRegionInfo.chongmingArea);
        chongmingDensityDiff.put("value", ((double) chongmingDisasters.size()) / FeiteRegionInfo.chongmingAcreage);

        JSONArray densityDiffValue = new JSONArray();
        densityDiffValue.add(yangpuDensityDiff);
        densityDiffValue.add(chongmingDensityDiff);

        DataEntity densityDiff = new DataEntity();
        densityDiff.setName(FeiteTaskName.FEITE_REGION_DISASTER_DENSITY_DIFF);
        densityDiff.setValue(densityDiffValue);

        dataDAO.updateExample(densityDiff);

        // 统计两地受灾种类数
        JSONObject yangpuDisasterType = getAreaDisasterType(FeiteRegionInfo.yangpuArea, yangpuDisasters);
        JSONObject chongmingDisasterType = getAreaDisasterType(FeiteRegionInfo.chongmingArea, chongmingDisasters);

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

    private void addRainfall(JSONArray areaRainfalls, JSONObject rainfall, int date) {
        JSONObject jsonObject = new JSONObject();
        double rainHour = Double.parseDouble((String) rainfall.get("RAINHOUR"));

        jsonObject.put("date", date);
        jsonObject.put("value", (int) (rainHour * 10));

        areaRainfalls.add(jsonObject);
    }

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
