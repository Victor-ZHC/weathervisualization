package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.BackUpDataDAO;
import com.adc.disasterforecast.dao.RainfallDataDAO;
import com.adc.disasterforecast.entity.RainfallDataEntity;
import com.adc.disasterforecast.global.BackUpDataName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.RainfallTaskName;
import com.adc.disasterforecast.tools.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

//@Component
public class RainfallTask {
    // logger for RealTimeControlTask
    private static final Logger logger = LoggerFactory.getLogger(RainfallTask.class);

    // dao Autowired
    @Autowired
    private RainfallDataDAO rainfallDataDAO;

    @Autowired
    private BackUpDataDAO backUpDataDAO;

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countSeeperSiteTOP10AndMax() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.SEEPER_SITE_TOP10));
            logger.info(String.format("began task：%s", RainfallTaskName.SEEPER_SITE_MAX));

            String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYPWaterStationData/";
            String date = DateHelper.getCurrentTimeInString("minute");
            String url = baseUrl + date;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");

            List<JSONObject> list = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                JSONObject station = (JSONObject) array.get(i);

                JSONObject seeper = new JSONObject();
                seeper.put("id", station.get("STATIONID"));
                seeper.put("site", station.get("STATIONNAME"));
                seeper.put("value", station.get("WATERDEPTH"));

                list.add(seeper);
            }

            Collections.sort(list, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    if (((Number) o1.get("value")).doubleValue() - ((Number) o2.get("value")).doubleValue() > 0){
                        return -1;
                    } else if (((Number) o1.get("value")).doubleValue() - ((Number) o2.get("value")).doubleValue() < 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            JSONArray value = new JSONArray();
            for (int i = 0; i < 10 && i < list.size(); i++) {
                value.add(list.get(i));
            }

            RainfallDataEntity seeperSiteTOP10 = new RainfallDataEntity();
            seeperSiteTOP10.setName(RainfallTaskName.SEEPER_SITE_TOP10);
            seeperSiteTOP10.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(seeperSiteTOP10);

            // get max
            String maxSeeperSetId = ((String) list.get(0).get("id")).trim();

            String maxBaseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYPWaterStationDataByTime/";
            String beginDate = DateHelper.getCurrentTimeInString("day");
            String maxurl = maxBaseUrl + beginDate + "/" + date;

            JSONObject maxObj = HttpHelper.getDataByURL(maxurl);
            JSONArray maxArray = (JSONArray) maxObj.get("Data");

            List<JSONObject> maxList = new ArrayList<>();
            for (int i = 0; i < maxArray.size(); i++) {
                JSONObject maxData = (JSONObject) maxArray.get(i);
                String seeperSetId = ((String) maxData.get("STATIONID")).trim();

                if (maxSeeperSetId.equals(seeperSetId)) {
                    JSONObject max = new JSONObject();
                    max.put("date", DateHelper.getDateInLong((String) maxData.get("DATATIME")));
                    max.put("value", maxData.get("WATERDEPTH"));
                    maxList.add(max);
                }
            }

            Collections.sort(maxList, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o1.get("date")).longValue() - ((Number) o2.get("date")).longValue() > 0 ? 1 : -1;
                }
            });

            JSONArray maxValue = new JSONArray();
            maxValue.addAll(maxList);

            RainfallDataEntity seeperSiteMax = new RainfallDataEntity();
            seeperSiteMax.setName(RainfallTaskName.SEEPER_SITE_MAX);
            seeperSiteMax.setValue(maxValue);
            rainfallDataDAO.updateRainfallDataByName(seeperSiteMax);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countNLInfluenceTOP10() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.NL_INFLUENCE_TOP10));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRiskAlarmByTimeAndUsername/ypq/";
            String beginDate = "20150101000000";
            String endDate = DateHelper.getCurrentTimeInString("day");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) ((JSONObject) obj.get("Data")).get("SubAlarms");

            Map<String, int[]> map = new HashMap<>();
            for (int i = 0; i < array.size(); i++) {
                JSONObject data = (JSONObject) array.get(i);

                if ("暴雨内涝风险预警".equals(data.get("TYPE")) && ! "解除".equals(data.get("OPERATION"))) {
                    String name = (String) data.get("Name");
                    int level = WarningHelper.getWarningInInt((String) data.get("LEVEL")) - 1;

                    if (map.containsKey(name)) {
                        int[] levels = map.get(name);
                        levels[level] += 1;
                        map.put(name, levels);
                    } else {
                        int[] levels = new int[4];
                        levels[level] += 1;
                        map.put(name, levels);
                    }
                }
            }

            List<JSONObject> list = new ArrayList<>();
            map.forEach((String k, int[] v) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("zone", k);
                jsonObject.put("level1", v[0]);
                jsonObject.put("level2", v[1]);
                jsonObject.put("level3", v[2]);
                jsonObject.put("level4", v[3]);
                jsonObject.put("sum", v[0] + v[1] + v[2] + v[3]);

                list.add(jsonObject);
            });

            Collections.sort(list, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o2.get("sum")).intValue() - ((Number) o1.get("sum")).intValue();
                }
            });

            JSONArray value = new JSONArray();
            for (int i = 0; i < 10 && i < list.size(); i++) {
                value.add(list.get(i));
            }

            RainfallDataEntity NLInfluenceTOP10 = new RainfallDataEntity();
            NLInfluenceTOP10.setName(RainfallTaskName.NL_INFLUENCE_TOP10);
            NLInfluenceTOP10.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(NLInfluenceTOP10);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @Scheduled(cron = "0 0 0 * * ?")
    public void countNLTOP10() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.NL_TOP10));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetPSQKForecastModelAreaByTime/";
            String beginDate = "20150101000000";
            String endDate = DateHelper.getCurrentTimeInString("day");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");

            Map<String, Integer> map = new HashMap<>();
            for (int i = 0; i < array.size(); i++) {
                JSONObject data = (JSONObject) array.get(i);

                if (! "0".equals(data.get("FloodArea"))) {
                    String name = (String) data.get("AreaName");

                    if (map.containsKey(name)) {
                        map.put(name, map.get(name) + 1);
                    } else {
                        map.put(name, 1);
                    }
                }
            }

            List<JSONObject> list = new ArrayList<>();
            map.forEach((String k, Integer v) -> {
                JSONObject jsonObject = new JSONObject();
                jsonObject.put("zone", k);
                jsonObject.put("value", v);

                list.add(jsonObject);
            });

            Collections.sort(list, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o2.get("value")).intValue() - ((Number) o1.get("value")).intValue();
                }
            });

            JSONArray value = new JSONArray();
            for (int i = 0; i < 10 && i < list.size(); i++) {
                value.add(list.get(i));
            }

            RainfallDataEntity NLTOP10 = new RainfallDataEntity();
            NLTOP10.setName(RainfallTaskName.NL_TOP10);
            NLTOP10.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(NLTOP10);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @Scheduled(cron = "0 0 0 * * ?")
    public void countSeeperRankTOP10() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.SEEPER_RANK_TOP10));

            JSONArray historySeeperValue = backUpDataDAO.findBackUpDataByName(BackUpDataName.SEEPER).getValue();

            List<Map<String, Number>> list = new ArrayList<>();
            list.addAll(historySeeperValue);

            Collections.sort(list, new Comparator<Map<String, Number>>() {
                @Override
                public int compare(Map<String, Number> o1, Map<String, Number> o2) {
                    if ((o1.get("value")).doubleValue() - (o2.get("value")).doubleValue() > 0) {
                        return -1;
                    } else if ((o1.get("value")).doubleValue() - (o2.get("value")).doubleValue() < 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            JSONArray value = new JSONArray();
            for (int i = 0; i < 10; i++) {
                value.add(list.get(i));
            }

            RainfallDataEntity seeperRankTOP10 = new RainfallDataEntity();
            seeperRankTOP10.setName(RainfallTaskName.SEEPER_RANK_TOP10);
            seeperRankTOP10.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(seeperRankTOP10);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @Scheduled(cron = "0 0 0 * * ?")
    public void countRainRankTOP10() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.RAIN_RANK_TOP10));

            JSONArray array = backUpDataDAO.findBackUpDataByName(BackUpDataName.RAINFALL).getValue();
            List<Map<String, Number>> list = new ArrayList<>();
            list.addAll(array);

            Collections.sort(list, new Comparator<Map<String, Number>>() {
                @Override
                public int compare(Map<String, Number> o1, Map<String, Number> o2) {
                    if ((o1.get("value")).doubleValue() - (o2.get("value")).doubleValue() > 0) {
                        return -1;
                    } else if ((o1.get("value")).doubleValue() - (o2.get("value")).doubleValue() < 0) {
                        return 1;
                    } else {
                        return 0;
                    }
                }
            });

            JSONArray value = new JSONArray();
            for (int i = 0; i < 10; i++) {
                Map<String, Number> maxRain = list.get(i);
                value.add(maxRain);
            }

            RainfallDataEntity rainRankTOP10 = new RainfallDataEntity();
            rainRankTOP10.setName(RainfallTaskName.RAIN_RANK_TOP10);
            rainRankTOP10.setValue(value);

            rainfallDataDAO.updateRainfallDataByName(rainRankTOP10);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countRainSiteTOP10AndMax() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.RAIN_SITE_TOP10));
            logger.info(String.format("began task：%s", RainfallTaskName.RAIN_SITE_MAX));

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/" +
                    beginDate + "/" + endDate + "/1";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray autoStationDataArray = (JSONArray) obj.get("Data");

            List<JSONObject> autoStationList = new ArrayList<>();
            Set<String> autoStation = StationHelper.getYPAutoStation();
            for (int i = 0; i < autoStationDataArray.size(); i++) {
                JSONObject autoStationData = (JSONObject) autoStationDataArray.get(i);

                if (autoStation.contains(autoStationData.get("STATIONID"))) {
                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("id", autoStationData.get("STATIONID"));
                    jsonObject.put("site", autoStationData.get("STATIONNAME"));
                    jsonObject.put("value", RainfallHelper.getRainHour((String) autoStationData.get("RAINHOUR")));

                    autoStationList.add(jsonObject);
                }

            }

            Collections.sort(autoStationList, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o1.get("value")).doubleValue() - ((Number) o2.get("value")).doubleValue() > 0 ? -1 : 1;
                }
            });


            JSONArray value = new JSONArray();
            for (int i = 0; i < 10 && i < autoStationList.size(); i++) {
                value.add(autoStationList.get(i));
            }

            RainfallDataEntity rainSiteTOP10 = new RainfallDataEntity();
            rainSiteTOP10.setName(RainfallTaskName.RAIN_SITE_TOP10);
            rainSiteTOP10.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(rainSiteTOP10);

            // 计算最大站点的实时情况
            String maxRainSet = (String) autoStationList.get(0).get("id");

            String maxEndDate = "";
            String stopDate = DateHelper.getCurrentTimeInString("hour");
            int delay = 1;

            List<JSONObject> maxAutoStationList = new ArrayList<>();
            while (! stopDate.equals(maxEndDate)) {
                maxEndDate = DateHelper.getPostponeDateByHour(beginDate, delay);
                String maxUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/" +
                        beginDate + "/" + maxEndDate + "/1";

                JSONObject maxObj = HttpHelper.getDataByURL(maxUrl);
                JSONArray maxArray = (JSONArray) maxObj.get("Data");

                for (int i = 0; i < maxArray.size(); i++) {
                    JSONObject maxAutoStationData = (JSONObject) maxArray.get(i);

                    if (maxRainSet.equals(maxAutoStationData.get("STATIONID"))) {

                        JSONObject maxObject = new JSONObject();
                        maxObject.put("date", DateHelper.getPostponeDateByHourInLong(maxEndDate, 0));
                        maxObject.put("value", RainfallHelper.getRainHour((String) maxAutoStationData.get("RAINHOUR")));
                        maxAutoStationList.add(maxObject);
                    }

                }
                delay++;
            }

            Collections.sort(maxAutoStationList, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o1.get("date")).longValue() - ((Number) o2.get("date")).longValue() > 0 ? 1 : -1;
                }
            });

            JSONArray maxValue = new JSONArray();
            maxValue.addAll(maxAutoStationList);

            RainfallDataEntity rainSiteMax = new RainfallDataEntity();
            rainSiteMax.setName(RainfallTaskName.RAIN_SITE_MAX);
            rainSiteMax.setValue(maxValue);
            rainfallDataDAO.updateRainfallDataByName(rainSiteMax);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @Scheduled(cron = "0 0 0 * * ?")
    public void countHistoryDisaster(){
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.HISTORY_DISASTER));
            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

            String beginDate = "20150101000000";
            String endDate = DateHelper.getCurrentTimeInString("day");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray disasterArray = (JSONArray) obj.get("Data");

            List<JSONObject> disasterList = new ArrayList<>();
            disasterList.addAll(disasterArray);

            JSONArray value = DisasterTypeHelper.getDisasterTypeInJsonArray(disasterList);

            RainfallDataEntity disaster = new RainfallDataEntity();
            disaster.setName(RainfallTaskName.HISTORY_DISASTER);
            disaster.setValue(value);

            rainfallDataDAO.updateRainfallDataByName(disaster);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countDisaster(){
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.DISASTER));
            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray disasterArray = (JSONArray) obj.get("Data");

            List<JSONObject> disasterList = new ArrayList<>();
            disasterList.addAll(disasterArray);

            JSONArray value = DisasterTypeHelper.getDisasterTypeInJsonArray(disasterList);

            RainfallDataEntity disaster = new RainfallDataEntity();
            disaster.setName(RainfallTaskName.DISASTER);
            disaster.setValue(value);

            rainfallDataDAO.updateRainfallDataByName(disaster);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countBYNLWarningAndNotice() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.BYNL_WARNING));
            logger.info(String.format("began task：%s", RainfallTaskName.WARNING_NOTICE));

            String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRiskAlarmByUsername/ypq";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONObject warningObj = (JSONObject) obj.get("Data");

            JSONArray value = new JSONArray();
            JSONArray noticeValue = new JSONArray();
            if ("暴雨内涝风险预警".equals(warningObj.get("TYPE")) && (!"解除".equals(warningObj.get("OPERATION")))) {
                JSONObject warning = new JSONObject();
                warning.put("level", WarningHelper.getWarningInInt((String) warningObj.get("LEVEL")));
                warning.put("count", 100);

                value.add(warning);

                JSONObject notice = new JSONObject();
                notice.put("level", WarningHelper.getWarningInColor((String) warningObj.get("LEVEL")));
                notice.put("value", warningObj.get("CONTENT"));
                noticeValue.add(notice);
            }

            RainfallDataEntity BYNLWarning = new RainfallDataEntity();
            BYNLWarning.setValue(value);
            BYNLWarning.setName(RainfallTaskName.BYNL_WARNING);
            rainfallDataDAO.updateRainfallDataByName(BYNLWarning);

            // notice
            RainfallDataEntity notice = new RainfallDataEntity();
            notice.setName(RainfallTaskName.WARNING_NOTICE);
            notice.setValue(noticeValue);
            rainfallDataDAO.updateRainfallDataByName(notice);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countSeeperAndDrain() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.SEEPER_TOP10));
            logger.info(String.format("began task：%s", RainfallTaskName.DRAIN_TOP10));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetPSQKForecastModelArea/";

            String queryTime = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + queryTime;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");

            List<JSONObject> seeperList = new ArrayList<>();
            List<JSONObject> drainList = new ArrayList<>();
            for (int i = 0; i < array.size(); i++) {
                JSONObject seeperAndDrain = (JSONObject) array.get(i);

                JSONObject seeperObject = new JSONObject();
                seeperObject.put("zone", seeperAndDrain.get("AreaName"));
                seeperObject.put("value", Double.valueOf((String) seeperAndDrain.get("FloodArea")));
                seeperList.add(seeperObject);

                JSONObject drainObject = new JSONObject();
                double ratio = Double.valueOf((String) seeperAndDrain.get("RATIO"));
                drainObject.put("zone", seeperAndDrain.get("AreaName"));
                drainObject.put("value", ratio);
                drainObject.put("level", getDrainLevel(ratio));
                drainList.add(drainObject);
            }

            Collections.sort(seeperList, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o1.get("value")).doubleValue() - ((Number) o2.get("value")).doubleValue() > 0 ? -1 : 1;
                }
            });

            Collections.sort(drainList, new Comparator<JSONObject>() {
                @Override
                public int compare(JSONObject o1, JSONObject o2) {
                    return ((Number) o1.get("value")).doubleValue() - ((Number) o2.get("value")).doubleValue() > 0 ? -1 : 1;
                }
            });

            JSONArray seeperValue = new JSONArray();
            for (int i = 0; i < 10 && i < seeperList.size(); i++) {
                seeperValue.add(seeperList.get(i));
            }

            JSONArray drainValue = new JSONArray();
            for (int i = 0; i < 10 && i < drainList.size(); i++) {
                drainValue.add(drainList.get(i));
            }

            RainfallDataEntity seeper = new RainfallDataEntity();
            seeper.setName(RainfallTaskName.SEEPER_TOP10);
            seeper.setValue(seeperValue);
            rainfallDataDAO.updateRainfallDataByName(seeper);

            RainfallDataEntity drain = new RainfallDataEntity();
            drain.setName(RainfallTaskName.DRAIN_TOP10);
            drain.setValue(drainValue);
            rainfallDataDAO.updateRainfallDataByName(drain);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private int getDrainLevel (double ratio) {
        if (ratio == 0) {
            return 5;
        } else if (ratio < 1.25) {
            return 4;
        } else if (ratio < 2.5) {
            return 3;
        } else if (ratio < 5){
            return 2;
        } else {
            return 1;
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countWarning() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.WARNING));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray earlyWarningDataArray = (JSONArray) obj.get("Data");

            Map<String, String> earlyWarningMap = WarningHelper.getEarlyWarningMap();

            for (int i = 0; i < earlyWarningDataArray.size(); i++) {
                JSONObject earlyWarningDataObject = (JSONObject) earlyWarningDataArray.get(i);

                String warningType = WarningHelper.getWarningWeather((String) earlyWarningDataObject.get("TYPE"));
                String warningLevel = WarningHelper.getWarningLevel((String) earlyWarningDataObject.get("LEVEL"));

                if (earlyWarningMap.containsKey(warningType)) {
                    earlyWarningMap.put(warningType, warningLevel);
                }

            }

            JSONArray earlyWarningValue = new JSONArray();
            earlyWarningMap.forEach((String k, String v) -> {
                JSONObject warning = new JSONObject();
                warning.put("type", k);
                warning.put("warning", v);
                earlyWarningValue.add(warning);
            });

            RainfallDataEntity warning = new RainfallDataEntity();
            warning.setName(RainfallTaskName.WARNING);
            warning.setValue(earlyWarningValue);
            rainfallDataDAO.updateRainfallDataByName(warning);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countDisasterSpread() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.DISASTER_SPREAD));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray disasterArray = (JSONArray) obj.get("Data");

            JSONArray value = new JSONArray();
            for (int i = 0; i < disasterArray.size(); i++) {
                JSONObject disasterObject = (JSONObject) disasterArray.get(i);

                int district = ((Number) disasterObject.get("DISTRICT")).intValue();

                if (district == 14) {
                    JSONObject object = new JSONObject();
                    JSONObject pos = new JSONObject();
                    pos.put("lon", disasterObject.get("LONTITUDE"));
                    pos.put("lat", disasterObject.get("LATITUDE"));
                    object.put("pos", pos);
                    value.add(object);
                }
            }

            RainfallDataEntity disasterArea = new RainfallDataEntity();
            disasterArea.setName(RainfallTaskName.DISASTER_SPREAD);
            disasterArea.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(disasterArea);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countSeeperSpread() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.SEEPER_SPREAD));

            String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetYPWaterStationData/";
            String date = DateHelper.getCurrentTimeInString("minute");
            String url = baseUrl + date;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");

            JSONArray value = new JSONArray();
            for (int i = 0; i < array.size(); i++) {
                JSONObject seeperStation = (JSONObject) array.get(i);

                JSONObject object = new JSONObject();
                JSONObject pos = new JSONObject();
                pos.put("lon", seeperStation.get("LON"));
                pos.put("lat", seeperStation.get("LAT"));
                object.put("pos", pos);

                object.put("name", seeperStation.get("STATIONNAME"));

                object.put("value", seeperStation.get("WATERDEPTH"));

                value.add(object);
            }

            RainfallDataEntity seeperStationArea = new RainfallDataEntity();
            seeperStationArea.setName(RainfallTaskName.SEEPER_SPREAD);
            seeperStationArea.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(seeperStationArea);

            logger.info(String.format("began task：%s", RainfallTaskName.SEEPER_SITE_COUNT));
            RainfallDataEntity stationCountEntity = new RainfallDataEntity();
            stationCountEntity.setName(RainfallTaskName.SEEPER_SITE_COUNT);
            JSONArray countValue = new JSONArray();
            countValue.add(value.size());
            stationCountEntity.setValue(countValue);
            rainfallDataDAO.updateRainfallDataByName(stationCountEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countRainSpread() {
        try {
            logger.info(String.format("began task：%s", RainfallTaskName.RAIN_SPREAD));

            String baseUrl = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/";

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + beginDate + "/" + endDate + "/1";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray autoStationArray = (JSONArray) obj.get("Data");
            Set<String> autoStation = StationHelper.getYPAutoStation();

            JSONArray value = new JSONArray();
            for (int i = 0; i < autoStationArray.size(); i++) {
                JSONObject autoStationObject = (JSONObject) autoStationArray.get(i);
                String autoStationId = (String) autoStationObject.get("STATIONID");

                if (autoStation.contains(autoStationId)) {
                    JSONObject object = new JSONObject();

                    JSONObject pos = new JSONObject();
                    pos.put("lon", Double.valueOf((String) autoStationObject.get("LON")));
                    pos.put("lat", Double.valueOf((String) autoStationObject.get("LAT")));
                    object.put("pos", pos);

                    object.put("name", autoStationObject.get("STATIONNAME"));

                    object.put("value", RainfallHelper.getRainHour((String) autoStationObject.get("RAINHOUR")));

                    value.add(object);
                }

            }

            RainfallDataEntity disasterArea = new RainfallDataEntity();
            disasterArea.setName(RainfallTaskName.RAIN_SPREAD);
            disasterArea.setValue(value);
            rainfallDataDAO.updateRainfallDataByName(disasterArea);

            logger.info(String.format("began task：%s", RainfallTaskName.RAIN_SITE_COUNT));
            RainfallDataEntity stationCountEntity = new RainfallDataEntity();
            stationCountEntity.setName(RainfallTaskName.RAIN_SITE_COUNT);
            JSONArray countValue = new JSONArray();
            countValue.add(value.size());
            stationCountEntity.setValue(countValue);
            rainfallDataDAO.updateRainfallDataByName(stationCountEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
