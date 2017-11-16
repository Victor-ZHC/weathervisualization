package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import com.adc.disasterforecast.global.FeiteRegionInfo;
import com.adc.disasterforecast.global.FeiteTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import java.util.*;
import javax.xml.crypto.Data;
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

    @Scheduled(cron = "0 0 * * * *")
    public void getRainfallTop10() throws InterruptedException{
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";
        String type = "1";
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_RAINFALL_TOP10));

        HashMap<String, LinkedList<Double> > hs =  new HashMap<>();
        for (int i = 0; i < FeiteRegionInfo.Hours; i++){
            String date = DateHelper.getPostponeDateByHour(2013, 10, 7, 13, 0, 0, i);
            String url = baseUrl + date + "/" + date + "/" + type;
            JSONObject rainfallJson = HttpHelper.getDataByURL(url);
            JSONArray rainfallData = (JSONArray) rainfallJson.get("Data");

            for (Object obj : rainfallData) {
                JSONObject rainfall = (JSONObject) obj;
                LinkedList<Double> allRainfall = hs.get(rainfall.get("STATIONNAME"));
                if (allRainfall == null) allRainfall = new LinkedList<>();
                allRainfall.add(Double.parseDouble((String)rainfall.get("RAINHOUR")));
                hs.put((String) rainfall.get("STATIONNAME"), allRainfall);
            }
        }
        JSONArray rainfallTop10 = new JSONArray();
        int cnt = 10;
        Iterator iter = hs.entrySet().iterator();
        while (iter.hasNext()){
            Map.Entry entry = (Map.Entry) iter.next();
            String siteName = (String)entry.getKey();
            LinkedList<Double> rainfallVal = (LinkedList<Double>)entry.getValue();
            Collections.sort(rainfallVal);
            JSONObject rainfallTopBySite = new JSONObject();
            rainfallTopBySite.put("site", siteName);
            rainfallTopBySite.put("value", rainfallVal.getLast());
            rainfallTop10.add(rainfallTopBySite);
            if ((--cnt) < 0) break;
        }

        DataEntity rainfall = new DataEntity();
        rainfall.setName(FeiteTaskName.FEITE_RAINFALL_TOP10);
        rainfall.setValue(rainfallTop10);
        dataDAO.updateExample(rainfall);
    }

    @Scheduled(cron = "0 0 * * * *")
    public void getGaleTop10() throws InterruptedException {
        String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";
        String type = "1";
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_GALE_TOP10));

        HashMap<String, LinkedList<Double>> hs = new HashMap<>();
        for (int i = 0; i < FeiteRegionInfo.Hours; i++) {
            String date = DateHelper.getPostponeDateByHour(2013, 10, 7, 13, 0, 0, i);
            String url = baseUrl + date + "/" + date + "/" + type;
            JSONObject GaleJson = HttpHelper.getDataByURL(url);
            JSONArray GaleData = (JSONArray) GaleJson.get("Data");

            for (Object obj : GaleData) {
                JSONObject Gale = (JSONObject) obj;
                LinkedList<Double> allGale = hs.get(Gale.get("STATIONNAME"));
                if (allGale == null) allGale = new LinkedList<>();
                allGale.add(Double.parseDouble((String) Gale.get("WINDSPEED")));
                hs.put((String) Gale.get("STATIONNAME"), allGale);
            }
        }
        JSONArray GaleTop10 = new JSONArray();
        int cnt = 10;
        Iterator iter = hs.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry entry = (Map.Entry) iter.next();
            String siteName = (String) entry.getKey();
            LinkedList<Double> rainfallVal = (LinkedList<Double>) entry.getValue();
            Collections.sort(rainfallVal);
            JSONObject GaleTopBySite = new JSONObject();
            GaleTopBySite.put("site", siteName);
            GaleTopBySite.put("value", rainfallVal.getLast());
            GaleTop10.add(GaleTopBySite);
            if ((--cnt) < 0) break;
        }

        DataEntity gale = new DataEntity();
        gale.setName(FeiteTaskName.FEITE_GALE_TOP10);
        gale.setValue(GaleTop10);
        dataDAO.updateExample(gale);
    }
    /**
    * @Description 预警（但接口无返回数据）
    * @Author lilin
    * @Create 2017/11/16 22:25
    **/

    @Scheduled(cron = "20 * * * * *")
    public void getWarning() throws InterruptedException {
        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "/GetWeatherWarnningByDatetime/20131006200000/20131008120000";
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_WARNING));

        JSONObject obj = HttpHelper.getDataByURL(url);

        JSONArray resultArray = new JSONArray();

        JSONArray warnings = (JSONArray) obj.get("Data");
        for (int i = 0; i < warnings.size(); i++) {
            JSONObject warning = (JSONObject) warnings.get(i);
            String date = (String) warning.get("FORECASTDATE");
            String weather = (String) warning.get("TYPE");
            String level = (String) warning.get("LEVEL");
            JSONObject resultObject = new JSONObject();
            resultObject.put("date", DateHelper.getWarningDate(date));
            resultObject.put("weather", WarningHelper.getWarningWeather(weather));
            resultObject.put("level", WarningHelper.getWarningLevel(level));
            resultArray.add(resultObject);
        }

        DataEntity warningsData = new DataEntity();
        warningsData.setName(FeiteTaskName.FEITE_WARNING);
        warningsData.setValue(resultArray);
        dataDAO.updateExample(warningsData);
    }

    /**
    * @Description 雨量累计&大风监测
    * @Author lilin
    * @Create 2017/11/16 17:51
    **/

    @Scheduled(cron = "20 * * * * *")
    public void countRainfallAndMonitorWind() throws InterruptedException {
        String url = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/20131006200000/20131008120000/1";
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_RAINFALL_TOTAL + " & " + FeiteTaskName.FEITE_GALE_TOTAL));

        JSONObject obj = HttpHelper.getDataByURL(url);

        JSONArray rainfallValueArray = new JSONArray();
        JSONArray windValueArray = new JSONArray();

        JSONArray autoStationDataArray = (JSONArray) obj.get("Data");
        for (int i = 0; i < autoStationDataArray.size(); i++) {
            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(i);
            String rainfallValue = (String) autoStationData.get("RAINHOUR");
            String windSpeedValue = (String) autoStationData.get("WINDSPEED");
            JSONObject rainfallValueObject = new JSONObject();
            rainfallValueObject.put("value", rainfallValue);
            rainfallValueObject.put("level", RainfallHelper.getRainfallLevel(rainfallValue));
            rainfallValueArray.add(rainfallValueObject);
            JSONObject windValueObject = new JSONObject();
            windValueObject.put("value", windSpeedValue);
            windValueObject.put("level", WindHelper.getWindLevel(windSpeedValue));
            windValueArray.add(windValueObject);
        }

        DataEntity rainfallTotalData = new DataEntity();
        rainfallTotalData.setName(FeiteTaskName.FEITE_RAINFALL_TOTAL);
        rainfallTotalData.setValue(rainfallValueArray);
        dataDAO.updateExample(rainfallTotalData);

        DataEntity galeTotalData = new DataEntity();
        galeTotalData.setName(FeiteTaskName.FEITE_GALE_TOTAL);
        galeTotalData.setValue(windValueArray);
        dataDAO.updateExample(galeTotalData);
    }

    /**
    * @Description 报灾情况
    * @Author lilin
    * @Create 2017/11/16 21:10
    **/
    @Scheduled(cron = "20 * * * * *")
    public void countDisasterReports() throws InterruptedException {
        String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "/GetDisasterDetailData_Geliku/20131006200000/20131008120000";
        logger.info(String.format("began task：%s", FeiteTaskName.FEITE_DISASTER_TOTAL));

        JSONObject obj = HttpHelper.getDataByURL(url);

        JSONObject resultObject = new JSONObject();
        JSONArray rainArray = new JSONArray();
        JSONArray windArray = new JSONArray();
        JSONArray resultArray = new JSONArray();

        JSONArray disasterReports = (JSONArray) obj.get("Data");
        int allNum = disasterReports.size();
        int rainNum = 0;
        int windNum = 0;
        // 房屋进水
        int FWJSNum = 0;
        // 道路积水
        int DLJSNum = 0;
        // 小区积水
        int XQJSNum = 0;
        // 车辆进水
        int CLJSNum = 0;
        // 厂区、商铺进水
        int CQSPJSNum = 0;
        // 其他
        int OtherNum = 0;
        // 风灾1
        int FZ1Num = 0;
        // 风灾2
        int FZ2Num = 0;
        for (int i = 0; i < disasterReports.size(); i++) {
            JSONObject disasterReport = (JSONObject) disasterReports.get(i);
            long disasterType = (long) disasterReport.get("CODE_DISASTER");
            String caseAddr = (String) disasterReport.get("CASE_ADDR");
            String caseDesc = (String) disasterReport.get("CASE_DESC");
            if (disasterType == 2) {
                windNum++;
                String windDisasterType = DisasterTypeHelper.getWindDisasterType(caseAddr, caseDesc);
                if ("风灾1".equals(windDisasterType)) {
                    FZ1Num++;
                } else {
                    FZ2Num++;
                }
            }
            if (disasterType == 1) {
                rainNum++;
                String rainstormDisasterType = DisasterTypeHelper.getRainstormDisasterType(caseAddr, caseDesc);
                if ("房屋进水".equals(rainstormDisasterType)) {
                    FWJSNum++;
                } else if ("道路积水".equals(rainstormDisasterType)) {
                    DLJSNum++;
                } else if ("小区积水".equals(rainstormDisasterType)) {
                    XQJSNum++;
                } else if ("车辆进水".equals(rainstormDisasterType)) {
                    CLJSNum++;
                } else if ("厂区、商铺进水".equals(rainstormDisasterType)) {
                    CQSPJSNum++;
                } else {
                    OtherNum++;
                }
            }
        }
        JSONObject totalValue = new JSONObject();
        totalValue.put("all", allNum);
        totalValue.put("rain", rainNum);
        totalValue.put("wind", windNum);
        resultObject.put("total", totalValue);

        JSONObject FWJSObject = new JSONObject();
        JSONObject DLJSObject = new JSONObject();
        JSONObject XQJSObject = new JSONObject();
        JSONObject CLJSObject = new JSONObject();
        JSONObject CQSPJSObject = new JSONObject();
        JSONObject OtherObject = new JSONObject();
        JSONObject FZ1Object = new JSONObject();
        JSONObject FZ2Object = new JSONObject();

        FWJSObject.put("type", "房屋进水");
        FWJSObject.put("value", FWJSNum + "");
        DLJSObject.put("type", "道路积水");
        DLJSObject.put("value", DLJSNum + "");
        XQJSObject.put("type", "小区积水");
        XQJSObject.put("value", XQJSNum + "");
        CLJSObject.put("type", "车辆进水");
        CLJSObject.put("value", CLJSNum + "");
        CQSPJSObject.put("type", "厂区、商铺进水");
        CQSPJSObject.put("value", CQSPJSNum + "");
        OtherObject.put("type", "其他");
        OtherObject.put("value", OtherNum + "");

        rainArray.add(FWJSObject);
        rainArray.add(DLJSObject);
        rainArray.add(XQJSObject);
        rainArray.add(CLJSObject);
        rainArray.add(CQSPJSObject);
        rainArray.add(OtherObject);

        resultObject.put("rain", rainArray);

        FZ1Object.put("type", "风灾1");
        FZ1Object.put("value", FZ1Num + "");
        FZ2Object.put("type", "风灾2");
        FZ2Object.put("value", FZ2Num + "");

        windArray.add(FZ1Object);
        windArray.add(FZ2Object);

        resultObject.put("wind", windArray);

        resultArray.add(resultObject);

        DataEntity disasterReportsData = new DataEntity();
        disasterReportsData.setName(FeiteTaskName.FEITE_DISASTER_TOTAL);
        disasterReportsData.setValue(resultArray);
        dataDAO.updateExample(disasterReportsData);
    }
}
