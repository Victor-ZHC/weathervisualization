package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.AirDataDAO;
import com.adc.disasterforecast.dao.RealTimeControlDAO;
import com.adc.disasterforecast.entity.AirDataEntity;
import com.adc.disasterforecast.entity.RealTimeControlDataEntity;
import com.adc.disasterforecast.global.AirTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.RealTimeControlTaskName;
import com.adc.disasterforecast.tools.*;
import org.apache.poi.ss.usermodel.Row;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.CachedIntrospectionResults;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.*;

@Component
public class RealTimeControlTask {
    // logger for RealTimeControlTask
    private static final Logger logger = LoggerFactory.getLogger(RealTimeControlTask.class);

    // dao Autowired
    @Autowired
    private RealTimeControlDAO realTimeControlDAO;
    @Autowired
    private AirDataDAO airDataDAO;

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countWeatherLive() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.WEATHER_LIVE));

            String todayUrl = JsonServiceURL.FORECAST_JSON_SERVICE_URL + "GetZXSForecast";
            JSONObject today = HttpHelper.getDataByURL(todayUrl);

            JSONArray todayDataList = (JSONArray) today.get("Data");

            JSONObject todayLive = new JSONObject();

            JSONObject todayData = (JSONObject) todayDataList.get(0);
            todayLive.put("weather", todayData.get("Weather"));
            todayLive.put("currentTemp", todayData.get("LowTmp"));

            String futureUrl = JsonServiceURL.FORECAST_JSON_SERVICE_URL + "Get10DayForecast";
            JSONObject future = HttpHelper.getDataByURL(futureUrl);

            JSONArray futureDataList = (JSONArray) future.get("Data");

            JSONArray weatherLiveValue = new JSONArray();

            for (int i = 0; i < 4; i++) {
                JSONObject futureData = (JSONObject) futureDataList.get(i);
                if (i == 0) {
                    todayLive.put("date", DateHelper.getPostponeDateByDay(i));
                    todayLive.put("minTemp", futureData.get("LowTmp"));
                    todayLive.put("maxTemp", futureData.get("HighTmp"));

                    weatherLiveValue.add(todayLive);
                } else {
                    JSONObject futureLive = new JSONObject();
                    futureLive.put("date", DateHelper.getPostponeDateByDay(i));
                    futureLive.put("weather", futureData.get("Day"));
                    futureLive.put("minTemp", futureData.get("LowTmp"));
                    futureLive.put("maxTemp", futureData.get("HighTmp"));

                    weatherLiveValue.add(futureLive);
                }
            }

            RealTimeControlDataEntity weatherLiveEntity = new RealTimeControlDataEntity();
            weatherLiveEntity.setValue(weatherLiveValue);
            weatherLiveEntity.setName(RealTimeControlTaskName.WEATHER_LIVE);

            realTimeControlDAO.updateRealTimeControlDataByName(weatherLiveEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countRainfallAndMonitorAndWind() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.RAINFALL_LIVE));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.WIND_LIVE));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.MONITORING_SITE));

            int[] delayHour = {1, 3, 6};

            String endDate = DateHelper.getCurrentTimeInString("hour");

            JSONArray rainfallLiveValue = new JSONArray();

            for (int i = 0; i < delayHour.length; i++) {
                String beginDate = DateHelper.getPostponeDateByHour(endDate, -delayHour[i]);

                String url = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "/GetAutoStationDataByDatetime_5mi_SanWei/" +
                        beginDate + "/" + endDate + "/1";

                JSONObject obj = HttpHelper.getDataByURL(url);
                JSONArray autoStationDataArray = (JSONArray) obj.get("Data");

                if (i == 0) {

                    JSONArray windValueArray = new JSONArray();
                    JSONArray monitorPointsNumArray = new JSONArray();

                    addRainfallLive(rainfallLiveValue, autoStationDataArray, delayHour[i]);
                    addWindLive(windValueArray, autoStationDataArray);
                    addMonitoringSite(monitorPointsNumArray, autoStationDataArray);

                    RealTimeControlDataEntity windLiveData = new RealTimeControlDataEntity();
                    windLiveData.setName(RealTimeControlTaskName.WIND_LIVE);
                    windLiveData.setValue(windValueArray);
                    realTimeControlDAO.updateRealTimeControlDataByName(windLiveData);

                    RealTimeControlDataEntity monitorPointsNumData = new RealTimeControlDataEntity();
                    monitorPointsNumData.setValue(monitorPointsNumArray);
                    monitorPointsNumData.setName(RealTimeControlTaskName.MONITORING_SITE);
                    realTimeControlDAO.updateRealTimeControlDataByName(monitorPointsNumData);
                } else {
                    addRainfallLive(rainfallLiveValue, autoStationDataArray, delayHour[i]);
                }
            }

            RealTimeControlDataEntity rainfallLiveData = new RealTimeControlDataEntity();
            rainfallLiveData.setName(RealTimeControlTaskName.RAINFALL_LIVE);
            rainfallLiveData.setValue(rainfallLiveValue);
            realTimeControlDAO.updateRealTimeControlDataByName(rainfallLiveData);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }

    }

    private void addRainfallLive(JSONArray rainfallLiveValue, JSONArray autoStationDataArray, int hours) {
        JSONArray rainfallValueArray = new JSONArray();

        Map<Integer, String> rainfallLevelMap = new HashMap();
        rainfallLevelMap.put(0, "0-50");
        rainfallLevelMap.put(1, "50-100");
        rainfallLevelMap.put(2, "100-150");
        rainfallLevelMap.put(3, "150-200");
        rainfallLevelMap.put(4, "200");

        Map<String, Integer> rainfallValueMap = new HashMap();
        for (int i = 0; i < 5; i++) {
            rainfallValueMap.put(rainfallLevelMap.get(i), 0);
        }

        for (int i = 0; i < autoStationDataArray.size(); i++) {
            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(i);
            String rainfallValue = (String) autoStationData.get("RAINHOUR");

            if (!rainfallLiveValue.equals("")) {
                double rainfallValueNum = Double.valueOf(rainfallValue);
                if (rainfallValueNum >= 0) {
                    String level = RainfallHelper.getRainfallLevel(rainfallValue);
                    Integer num = rainfallValueMap.get(level);
                    num ++;
                    rainfallValueMap.put(level, num);
                }
            }
        }

        for (int i = 0; i < 5; i++) {
            String level = rainfallLevelMap.get(i);
            JSONObject rainfallValueObject = new JSONObject();
            rainfallValueObject.put("level", level);
            rainfallValueObject.put("value", rainfallValueMap.get(level));
            rainfallValueArray.add(rainfallValueObject);
        }

        JSONObject rainfallLiveObject = new JSONObject();
        rainfallLiveObject.put("hours", hours);
        rainfallLiveObject.put("value", rainfallValueArray);
        rainfallLiveValue.add(rainfallLiveObject);
    }

    private void addWindLive(JSONArray windValueArray, JSONArray autoStationDataArray) {
        Map<String, Integer> windSpeedValueMap = new HashMap();
        for (int i = 0; i < 18; i++) {
            windSpeedValueMap.put(i + "", 0);
        }

        for (int i = 0; i < autoStationDataArray.size(); i++) {
            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(i);
            String windSpeedValue = (String) autoStationData.get("WINDSPEED");

            if (!windSpeedValue.equals("")) {
                double windSpeedValueNum = Double.valueOf(windSpeedValue);
                if (windSpeedValueNum >= 0) {
                    String level = WindHelper.getWindLevel(windSpeedValue);
                    Integer num = windSpeedValueMap.get(level);
                    num ++;
                    windSpeedValueMap.put(level, num);
                }
            }
        }

        for (int i = 0; i < 18; i++) {
            String level = i + "";
            JSONObject windSpeedValueObject = new JSONObject();
            windSpeedValueObject.put("level", level);
            windSpeedValueObject.put("value", windSpeedValueMap.get(level));
            windValueArray.add(windSpeedValueObject);
        }
    }

    private void addMonitoringSite(JSONArray monitorPointsNumArray, JSONArray autoStationDataArray) {
        int rainfallMonitorPointsNum = 0;
        int windSpeedMonitorPointsNum = 0;

        for (int i = 0; i < autoStationDataArray.size(); i++) {
            JSONObject autoStationData = (JSONObject) autoStationDataArray.get(i);
            String rainfallValue = (String) autoStationData.get("RAINHOUR");
            String windSpeedValue = (String) autoStationData.get("WINDSPEED");

            rainfallMonitorPointsNum += monitoringSiteAvailable(rainfallValue);
            windSpeedMonitorPointsNum += monitoringSiteAvailable(windSpeedValue);
        }

        JSONObject rainfallMonitorPointsNumObject = new JSONObject();
        rainfallMonitorPointsNumObject.put("rain", rainfallMonitorPointsNum);
        JSONObject windSpeedMonitorPointsNumObject = new JSONObject();
        windSpeedMonitorPointsNumObject.put("wind", windSpeedMonitorPointsNum);
        monitorPointsNumArray.add(rainfallMonitorPointsNumObject);
        monitorPointsNumArray.add(windSpeedMonitorPointsNumObject);
    }

    private int monitoringSiteAvailable(String value) {
        if (! value.equals("")) {
            double doubleValue = Double.valueOf(value);
            if (doubleValue < 0) {
                return 0;
            } else {
                return 1;
            }
        } else {
            return 0;
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countThunderLive() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.THUNDER_LIVE));

            String baseUrl = JsonServiceURL.THUNDER_JSON_SERVICE_URL + "Get_1H_Lightning/";

            JSONArray thunderLiveArray = new JSONArray();
            String baseDate = DateHelper.getCurrentTimeInString("hour");
            for (int i = 0; i < 24; i++) {
                String endDate = DateHelper.getPostponeDateByHour(baseDate, -i);
                String url = baseUrl + endDate;

                JSONObject obj = HttpHelper.getDataByURL(url);
                JSONArray thunderData = (JSONArray) obj.get("Data");

                JSONObject thunderLiveObject = new JSONObject();
                thunderLiveObject.put("date", DateHelper.getPostponeDateByHourInLong(baseDate, -i));
                thunderLiveObject.put("value", thunderData.size());

                thunderLiveArray.add(thunderLiveObject);
            }

            RealTimeControlDataEntity thunderLiveData = new RealTimeControlDataEntity();
            thunderLiveData.setName(RealTimeControlTaskName.THUNDER_LIVE);
            thunderLiveData.setValue(thunderLiveArray);
            realTimeControlDAO.updateRealTimeControlDataByName(thunderLiveData);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countMonitorStatsLive() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.MONITOR_STATS_LIVE));

            String endDate = DateHelper.getCurrentTimeInString("minute");
            String beginDate = DateHelper.getPostponeDateByHour(endDate, -1);

            String url = JsonServiceURL.AUTO_STATION_JSON_SERVICE_URL + "GetAutoStationDataByDatetime_5mi_SanWei/" +
                    beginDate + "/" + endDate + "/1";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray autoStationDataArray = (JSONArray) obj.get("Data");

            JSONArray monitorStatsValue = new JSONArray();
            for (int i = 0; i < autoStationDataArray.size(); i++) {
                JSONObject autoStationDataObject = (JSONObject) autoStationDataArray.get(i);

                if ("小洋山".equals(autoStationDataObject.get("STATIONNAME")))
                    continue;

                JSONObject autoStationData = new JSONObject();

                JSONObject stationPos = new JSONObject();
                stationPos.put("lon", Double.parseDouble((String) autoStationDataObject.get("LON")));
                stationPos.put("lat", Double.parseDouble((String) autoStationDataObject.get("LAT")));
                autoStationData.put("sitePos", stationPos);

                autoStationData.put("siteName", autoStationDataObject.get("STATIONNAME"));

                JSONObject siteRain = new JSONObject();
                String rainHour = (String) autoStationDataObject.get("RAINHOUR");
                siteRain.put("amount", RainfallHelper.getRainHour(rainHour));
                siteRain.put("level", RainfallHelper.getRainfallColor(rainHour));
                autoStationData.put("site_rain", siteRain);

                JSONObject siteWind = new JSONObject();
                String windSpeed = (String) autoStationDataObject.get("WINDSPEED");
                siteWind.put("amount", WindHelper.getWindSpeed(windSpeed));
                siteWind.put("level", WindHelper.getWindColor(windSpeed));
                autoStationData.put("site_wind", siteWind);

                monitorStatsValue.add(autoStationData);
            }

            RealTimeControlDataEntity monitorStats = new RealTimeControlDataEntity();
            monitorStats.setName(RealTimeControlTaskName.MONITOR_STATS_LIVE);
            monitorStats.setValue(monitorStatsValue);
            realTimeControlDAO.updateRealTimeControlDataByName(monitorStats);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countThunderStatsLive() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.THUNDER_STATS_LIVE));

            String endDate = DateHelper.getCurrentTimeInString("minute");
            String beginDate = DateHelper.getPostponeDateByMinute(endDate, -10);

            String url = JsonServiceURL.THUNDER_JSON_SERVICE_URL + "GetThunderData/ADTD/" +
                    beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray thunderDataArray = (JSONArray) obj.get("Data");

            JSONArray thunderStatsLiveValue = new JSONArray();
            for (int i = 0; i < thunderDataArray.size(); i++) {
                JSONObject thunderDataObject = (JSONObject) thunderDataArray.get(i);

                JSONObject thunderData = new JSONObject();

                JSONObject stationPos = new JSONObject();
                stationPos.put("lon", Double.parseDouble((String) thunderDataObject.get("LON")));
                stationPos.put("lat", Double.parseDouble((String) thunderDataObject.get("LAT")));
                thunderData.put("sitePos", stationPos);

                thunderData.put("time", DateHelper.getDateInLong((String) thunderDataObject.get("DATETIME")));

                thunderData.put("strength", thunderDataObject.get("PEAK_KA"));

                thunderData.put("level", "red");

                thunderStatsLiveValue.add(thunderData);
            }

            RealTimeControlDataEntity thunderStatsLive = new RealTimeControlDataEntity();
            thunderStatsLive.setName(RealTimeControlTaskName.THUNDER_STATS_LIVE);
            thunderStatsLive.setValue(thunderStatsLiveValue);
            realTimeControlDAO.updateRealTimeControlDataByName(thunderStatsLive);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countBroadcastV1() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.BROADCAST_V1));

            String url = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnning";

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray warningBroadcastDataArray = (JSONArray) obj.get("Data");

            JSONArray broadcastValue = new JSONArray();
            for (int i = 0; i < warningBroadcastDataArray.size(); i++) {
                JSONObject warningBroadcastDataObject = (JSONObject) warningBroadcastDataArray.get(i);
                broadcastValue.add(warningBroadcastDataObject.get("CONTENT"));
            }

            RealTimeControlDataEntity broadcastV1 = new RealTimeControlDataEntity();
            broadcastV1.setName(RealTimeControlTaskName.BROADCAST_V1);
            broadcastV1.setValue(broadcastValue);
            realTimeControlDAO.updateRealTimeControlDataByName(broadcastV1);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countBroadcastV2() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.BROADCAST_V2));

            RealTimeControlDataEntity entity = realTimeControlDAO.findRealTimeControlDataByName("All_ACTIVITIES");
            JSONArray allActivities = entity.getValue();
            int mouth = Calendar.getInstance().get(Calendar.MONTH);

            JSONArray broadcastV2Value = new JSONArray();
            broadcastV2Value.addAll(((Map<String, List>) allActivities.get(mouth)).get("content"));

            RealTimeControlDataEntity broadcastV2 = new RealTimeControlDataEntity();
            broadcastV2.setName(RealTimeControlTaskName.BROADCAST_V2);
            broadcastV2.setValue(broadcastV2Value);
            realTimeControlDAO.updateRealTimeControlDataByName(broadcastV2);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @EventListener(ApplicationReadyEvent.class)
    @PostConstruct
    public void countWarningRiskForecast() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.WARNING_RISK_FORECAST));

            // 获取健康预警
            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL + "GetHealthyMeteorological";
            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) obj.get("Data");
            JSONObject healthJo = (JSONObject) array.stream().max(Comparator.comparing(
                    o -> Integer.parseInt((String)((JSONObject)(((JSONArray)((JSONObject) o).get("Deatails")).get(0))).get("WarningLevel"))
            )).get();
            int healthLevel = Integer.parseInt((String)((JSONObject)(((JSONArray)healthJo.get("Deatails")).get(0))).get("WarningLevel"));

            // 获取航空预警
            AirDataEntity airDataEntity = airDataDAO.findAirDataByName(AirTaskName.HKQX_AIRPORT_CAPACTIY);
            JSONObject airData = (JSONObject) airDataEntity.getValue().get(0);
            int pudongLevel = (int) ((JSONObject)((JSONObject) airData.get("pudong"))).get("level");
            int hongqiaoLevel = (int) ((JSONObject)((JSONObject) airData.get("hongqiao"))).get("level");
            int maxAirportLevel = Math.max(pudongLevel, hongqiaoLevel);
            // 1,2,3,4 -> 5,4,3,1
            int airLevel;
            if (maxAirportLevel == 1) airLevel = 5;
            else if (maxAirportLevel == 2) airLevel = 4;
            else if (maxAirportLevel == 3) airLevel = 3;
            else airLevel = 1;

            JSONObject data = new JSONObject();
            data.put("baoyuneilao", "normal");
            data.put("hangkongqixiang", matchWarningLevel(airLevel));
            data.put("jiankangqixiang", matchWarningLevel(healthLevel));
            data.put("jiaotongqixiang", "normal");
            data.put("haiyangqixiang", "normal");

            JSONArray warningRiskForecastValue = new JSONArray();
            warningRiskForecastValue.add(data);

            RealTimeControlDataEntity warningRiskForecast = new RealTimeControlDataEntity();
            warningRiskForecast.setName(RealTimeControlTaskName.WARNING_RISK_FORECAST);
            warningRiskForecast.setValue(warningRiskForecastValue);
            realTimeControlDAO.updateRealTimeControlDataByName(warningRiskForecast);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countEarlyWarning() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.EARLY_WARNING));

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

            RealTimeControlDataEntity earlyWarning = new RealTimeControlDataEntity();
            earlyWarning.setName(RealTimeControlTaskName.EARLY_WARNING);
            earlyWarning.setValue(earlyWarningValue);
            realTimeControlDAO.updateRealTimeControlDataByName(earlyWarning);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countFocusActivities() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.FOCUS_ACTIVITIES));

            RealTimeControlDataEntity entity = realTimeControlDAO.findRealTimeControlDataByName("All_ACTIVITIES");
            JSONArray allActivities = entity.getValue();
            int mouth = Calendar.getInstance().get(Calendar.MONTH);

            JSONArray focusActivitiesValue = new JSONArray();
            focusActivitiesValue.addAll(((Map<String, List>) allActivities.get(mouth)).get("title"));

            RealTimeControlDataEntity focusActivities = new RealTimeControlDataEntity();
            focusActivities.setName(RealTimeControlTaskName.FOCUS_ACTIVITIES);
            focusActivities.setValue(focusActivitiesValue);
            realTimeControlDAO.updateRealTimeControlDataByName(focusActivities);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countDisaster() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.DISASTER_LIVE));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.DISASTER_AREA));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.DISASTER_TIME_PERIOD));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray disasterArray = (JSONArray) obj.get("Data");

            addDisasterLive(disasterArray);
            addDisasterArea(disasterArray);
            addDisasterTime(disasterArray);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void addDisasterLive(JSONArray disasterArray) {
        Map<String, Integer> disasterMap = DisasterTypeHelper.getDisasterMap();
        int totalNum = 0;
        for (int i = 0; i < disasterArray.size(); i++) {
            JSONObject disasterObject = (JSONObject) disasterArray.get(i);

            String disasterType = DisasterTypeHelper.getDisasterTypeByCode(((Number) disasterObject.get("CODE_DISASTER")).intValue());

            if (disasterMap.containsKey(disasterType)) {
                totalNum++;
                disasterMap.put(disasterType, disasterMap.get(disasterType) + 1);
            }
        }

        JSONObject disasterLiveObject = new JSONObject();
        disasterMap.forEach((String k, Integer v) -> disasterLiveObject.put(k, v));
        disasterLiveObject.put("total", totalNum);
        JSONArray disasterLiveValue = new JSONArray();
        disasterLiveValue.add(disasterLiveObject);

        RealTimeControlDataEntity disasterLive = new RealTimeControlDataEntity();
        disasterLive.setName(RealTimeControlTaskName.DISASTER_LIVE);
        disasterLive.setValue(disasterLiveValue);
        realTimeControlDAO.updateRealTimeControlDataByName(disasterLive);
    }

    private void addDisasterArea(JSONArray disasterArray) {
        Map<String, Integer> disasterAreaMap = AreaHelper.getAreaMap();

        for (int i = 0; i < disasterArray.size(); i++) {
            JSONObject disasterObject = (JSONObject) disasterArray.get(i);

            String district = AreaHelper.getDistrictByCode(((Number) disasterObject.get("DISTRICT")).intValue());

            if (disasterAreaMap.containsKey(district)) {
                disasterAreaMap.put(district, disasterAreaMap.get(district) + 1);
            }
        }

        JSONArray disasterAreaValue = new JSONArray();
        disasterAreaMap.forEach((String k, Integer v) -> {
            JSONObject disasterAreaObject = new JSONObject();
            disasterAreaObject.put("area", k);
            disasterAreaObject.put("value", v);
            disasterAreaValue.add(disasterAreaObject);
        });

        RealTimeControlDataEntity disasterArea = new RealTimeControlDataEntity();
        disasterArea.setName(RealTimeControlTaskName.DISASTER_AREA);
        disasterArea.setValue(disasterAreaValue);
        realTimeControlDAO.updateRealTimeControlDataByName(disasterArea);
    }

    private void addDisasterTime(JSONArray disasterArray) {
        Map<Long, Integer> disasterTimeMap = new HashMap<>();

        int hour = Calendar.getInstance().get(Calendar.HOUR_OF_DAY);
        String time = DateHelper.getCurrentTimeInString("day");
        for (int i = 0; i < hour + 1; i++) {
            disasterTimeMap.put(DateHelper.getDateInLongByHour(time, i), 0);
        }

        for (int i = 0; i < disasterArray.size(); i++) {
            JSONObject disasterObject = (JSONObject) disasterArray.get(i);

            long date =  DateHelper.getDateInLongByHour((String) disasterObject.get("DATETIME_DISASTER"));

            disasterTimeMap.put(date, disasterTimeMap.get(date) + 1);
        }

        List<JSONObject> disasterTimeList = new ArrayList<>();
        disasterTimeMap.forEach((Long k, Integer v) -> {
            JSONObject disasterTimeObject = new JSONObject();
            disasterTimeObject.put("time", k);
            disasterTimeObject.put("value", v);
            disasterTimeList.add(disasterTimeObject);
        });

        Collections.sort(disasterTimeList, new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {

                return (int) (((Number) o1.get("time")).longValue() - ((Number) o2.get("time")).longValue());
            }
        });

        JSONArray disasterTimeValue = new JSONArray();
        disasterTimeValue.addAll(disasterTimeList);

        RealTimeControlDataEntity disasterTime = new RealTimeControlDataEntity();
        disasterTime.setName(RealTimeControlTaskName.DISASTER_TIME_PERIOD);
        disasterTime.setValue(disasterTimeValue);
        realTimeControlDAO.updateRealTimeControlDataByName(disasterTime);
    }

//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countHistoryWarning() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_WARNING));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_WARNING_AVG));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_WARNING_MONTH));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";

            // 获取开始年份
            int baseTime = Calendar.getInstance().get(Calendar.YEAR) - 10;
            String beginTime = (baseTime > 2016 ? baseTime : 2016) + "0101000000";
            String endTime = DateHelper.getCurrentTimeInString("year");

            String url = baseUrl + beginTime + "/" + endTime;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray historyWarningArray = (JSONArray) obj.get("Data");

            List<Row> historyWarningFromExcel = ExcelHelper.loadAllExcelFile();

            for (Row row : historyWarningFromExcel) {
                String content = ExcelHelper.getCellContent(row, 0);

                if (content.contains("发布") && baseTime <= ExcelHelper.getWarningYear(content)) {
                    String date = ExcelHelper.getWarningDate(content);
                    String type = ExcelHelper.getWarningType(content);

                    JSONObject jsonObject = new JSONObject();
                    jsonObject.put("FORECASTDATE", date);
                    jsonObject.put("TYPE", type);
                    jsonObject.put("OPERATION", "发布");

                    historyWarningArray.add(jsonObject);
                }

            }

            int total = addHistoryWarningAndGetTotalYear(historyWarningArray);
            addHistoryWarningAvg(historyWarningArray, total);
            addHistoryWarningMouth(historyWarningArray, total);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private int addHistoryWarningAndGetTotalYear(JSONArray historyWarningArray) {
        Map<Integer, Integer> historyWarningMap = new HashMap<>();
        for (int i = 0; i < historyWarningArray.size(); i++) {
            JSONObject historyWarning = (JSONObject) historyWarningArray.get(i);

            int warningYear = Integer.valueOf(DateHelper.getYear((String) historyWarning.get("FORECASTDATE")));
            String warningOperation = (String) historyWarning.get("OPERATION");

            if (historyWarningMap.containsKey(warningYear)) {
                if ("发布".equals(warningOperation)) {
                    historyWarningMap.put(warningYear, historyWarningMap.get(warningYear) + 1);
                }
            } else {
                if ("发布".equals(warningOperation)) {
                    historyWarningMap.put(warningYear, 1);
                }
            }
        }

        List<JSONObject> historyWarningList = new ArrayList<>();
        historyWarningMap.forEach((Integer k, Integer v) -> {
            JSONObject historyWarningObject = new JSONObject();
            historyWarningObject.put("time", k);
            historyWarningObject.put("value", v);
            historyWarningList.add(historyWarningObject);
        });

        Collections.sort(historyWarningList, new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                return ((Number) o1.get("time")).intValue() - ((Number) o2.get("time")).intValue();
            }
        });

        JSONArray historyWarningValue = new JSONArray();
        historyWarningValue.addAll(historyWarningList);

        RealTimeControlDataEntity historyWarning = new RealTimeControlDataEntity();
        historyWarning.setName(RealTimeControlTaskName.HISTORY_WARNING);
        historyWarning.setValue(historyWarningValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyWarning);

        return historyWarningList.size();
    }

    private void addHistoryWarningAvg(JSONArray historyWarningArray, int total) {
        Map<String, Integer> historyWarningAvgMap = WarningHelper.getWarningMap();
        int totalNum = 0;
        for (int i = 0; i < historyWarningArray.size(); i++) {
            JSONObject historyWarning = (JSONObject) historyWarningArray.get(i);

            String warningType = WarningHelper.getWarningWeather((String) historyWarning.get("TYPE"));
            String warningOperation = (String) historyWarning.get("OPERATION");

            if (historyWarningAvgMap.containsKey(warningType) && "发布".equals(warningOperation)) {
                totalNum++;
                historyWarningAvgMap.put(warningType, historyWarningAvgMap.get(warningType) + 1);
            }

        }

        JSONArray historyWarningAvgValue = new JSONArray();
        JSONObject historyWarningAvgObject = new JSONObject();
        historyWarningAvgMap.forEach((k, v) -> historyWarningAvgObject.put(k, ((double) v) / total));
        historyWarningAvgObject.put("total", ((double) totalNum) / total);
        historyWarningAvgValue.add(historyWarningAvgObject);

        RealTimeControlDataEntity historyWarningAvg = new RealTimeControlDataEntity();
        historyWarningAvg.setName(RealTimeControlTaskName.HISTORY_WARNING_AVG);
        historyWarningAvg.setValue(historyWarningAvgValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyWarningAvg);
    }

    private void addHistoryWarningMouth(JSONArray historyWarningArray, int total) {
        Map<String, Integer> historyWarningMouthMap = WarningHelper.getWarningMap();
        int totalNum = 0;
        for (int i = 0; i < historyWarningArray.size(); i++) {
            JSONObject historyWarning = (JSONObject) historyWarningArray.get(i);

            if (String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1).equals(DateHelper.getMonth((String) historyWarning.get("FORECASTDATE")))) {
                String warningType = WarningHelper.getWarningWeather((String) historyWarning.get("TYPE"));
                String warningOperation = (String) historyWarning.get("OPERATION");
                if (historyWarningMouthMap.containsKey(warningType) && "发布".equals(warningOperation)) {
                    historyWarningMouthMap.put(warningType, historyWarningMouthMap.get(warningType) + 1);
                    totalNum++;
                }

            }
        }

        JSONArray historyWarningMonthValue = new JSONArray();
        JSONObject historyWarningMonthObject = new JSONObject();
        historyWarningMouthMap.forEach((String k, Integer v) -> historyWarningMonthObject.put(k, ((double) v) / total));
        historyWarningMonthObject.put("total", ((double) totalNum) / total);
        historyWarningMonthValue.add(historyWarningMonthObject);

        RealTimeControlDataEntity historyWarningMonth = new RealTimeControlDataEntity();
        historyWarningMonth.setName(RealTimeControlTaskName.HISTORY_WARNING_MONTH);
        historyWarningMonth.setValue(historyWarningMonthValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyWarningMonth);
    }


//    @Scheduled(initialDelay = 0, fixedDelay = 86400000)
    @PostConstruct
    @Scheduled(cron = "0 0 0 * * ?")
    public void countHistoryDisaster() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_DISASTER));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_DISASTER_AVG));
            logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_DISASTER_MONTH));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

            // 获取开始年份
            String beginTime = (Calendar.getInstance().get(Calendar.YEAR) - 10) + "0101000000";
            String endTime = DateHelper.getCurrentTimeInString("year");

            String url = baseUrl + beginTime + "/" + endTime;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray historyDisasterArray = (JSONArray) obj.get("Data");

            int total = addHistoryDisasterAndGetTotalYear(historyDisasterArray);
            addHistoryDisasterAvg(historyDisasterArray, total);
            addHistoryDisasterMouth(historyDisasterArray, total);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private int addHistoryDisasterAndGetTotalYear(JSONArray historyDisasterArray) {
        Map<Integer, Integer> historyWarningMap = new HashMap<>();
        for (int i = 0; i < historyDisasterArray.size(); i++) {
            JSONObject historyDisaster = (JSONObject) historyDisasterArray.get(i);

            int disasterYear = Integer.valueOf(DateHelper.getYear((String) historyDisaster.get("DATETIME_DISASTER")));

            if (historyWarningMap.containsKey(disasterYear)) {
                historyWarningMap.put(disasterYear, historyWarningMap.get(disasterYear) + 1);
            } else {
                historyWarningMap.put(disasterYear, 1);
            }
        }

        List<JSONObject> historyDisasterList = new ArrayList<>();
        historyWarningMap.forEach((k, v) -> {
            JSONObject historyDisasterObject = new JSONObject();
            historyDisasterObject.put("time", k);
            historyDisasterObject.put("value", v);
            historyDisasterList.add(historyDisasterObject);
        });

        Collections.sort(historyDisasterList, new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                return ((Number) o1.get("time")).intValue() - ((Number) o2.get("time")).intValue();
            }
        });

        JSONArray historyDisasterValue = new JSONArray();
        historyDisasterValue.addAll(historyDisasterList);

        RealTimeControlDataEntity historyDisaster = new RealTimeControlDataEntity();
        historyDisaster.setName(RealTimeControlTaskName.HISTORY_DISASTER);
        historyDisaster.setValue(historyDisasterValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyDisaster);

        return historyDisasterList.size();
    }

    private void addHistoryDisasterAvg(JSONArray historyDisasterArray, int total) {
        Map<String, Integer> historyDisasterAvgMap = DisasterTypeHelper.getDisasterMap();
        int totalNum = 0;
        for (int i = 0; i < historyDisasterArray.size(); i++) {
            JSONObject historyDisaster = (JSONObject) historyDisasterArray.get(i);

            String disasterType = DisasterTypeHelper.getDisasterTypeByCode(((Number) historyDisaster.get("CODE_DISASTER")).intValue());

            if (historyDisasterAvgMap.containsKey(disasterType)) {
                historyDisasterAvgMap.put(disasterType, historyDisasterAvgMap.get(disasterType) + 1);
                totalNum++;
            }

        }

        JSONArray historyDisasterAvgValue = new JSONArray();
        JSONObject historyDisasterAvgObject = new JSONObject();
        historyDisasterAvgMap.forEach((String k, Integer v) -> historyDisasterAvgObject.put(k, ((double) v) / total));
        historyDisasterAvgObject.put("total", ((double) totalNum) / total);
        historyDisasterAvgValue.add(historyDisasterAvgObject);

        RealTimeControlDataEntity historyDisasterAvg = new RealTimeControlDataEntity();
        historyDisasterAvg.setName(RealTimeControlTaskName.HISTORY_DISASTER_AVG);
        historyDisasterAvg.setValue(historyDisasterAvgValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyDisasterAvg);
    }

    private void addHistoryDisasterMouth(JSONArray historyDisasterArray, int total) {
        Map<String, Integer> historyDisasterMouthMap = DisasterTypeHelper.getDisasterMap();
        int totalNum = 0;
        for (int i = 0; i < historyDisasterArray.size(); i++) {
            JSONObject historyDisaster = (JSONObject) historyDisasterArray.get(i);

            if (String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1).equals(DateHelper.getMonth((String) historyDisaster.get("DATETIME_DISASTER")))) {
                String disasterType = DisasterTypeHelper.getDisasterTypeByCode(((Number) historyDisaster.get("CODE_DISASTER")).intValue());

                if (historyDisasterMouthMap.containsKey(disasterType)) {
                    totalNum++;
                    historyDisasterMouthMap.put(disasterType, historyDisasterMouthMap.get(disasterType) + 1);
                }
            }
        }

        JSONArray historyDisasterMonthValue = new JSONArray();
        JSONObject historyDisasterMonthObject = new JSONObject();
        historyDisasterMouthMap.forEach((String k, Integer v) -> historyDisasterMonthObject.put(k, ((double) v) / total));
        historyDisasterMonthObject.put("total", ((double) totalNum) / total);
        historyDisasterMonthValue.add(historyDisasterMonthObject);

        RealTimeControlDataEntity historyDisasterMonth = new RealTimeControlDataEntity();
        historyDisasterMonth.setName(RealTimeControlTaskName.HISTORY_DISASTER_MONTH);
        historyDisasterMonth.setValue(historyDisasterMonthValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyDisasterMonth);
    }

    @PostConstruct
//    @Scheduled(initialDelay = 0, fixedDelay = 600000)
    @Scheduled(cron = "0 0/10 * * * ?")
    public void countDisasterSpread() {
        try {
            logger.info(String.format("began task：%s", RealTimeControlTaskName.MONITOR_DISASTER_SPREAD));

            String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

            String beginDate = DateHelper.getCurrentTimeInString("day");
            String endDate = DateHelper.getCurrentTimeInString("minute");

            String url = baseUrl + beginDate + "/" + endDate;

            JSONObject obj = HttpHelper.getDataByURL(url);
            JSONArray disasterArray = (JSONArray) obj.get("Data");

            JSONArray value = new JSONArray();
            for (int i = 0; i < disasterArray.size(); i++) {
                JSONObject disasterObject = (JSONObject) disasterArray.get(i);

                JSONObject object = new JSONObject();
                object.put("lon", disasterObject.get("LONTITUDE"));
                object.put("lat", disasterObject.get("LATITUDE"));

                value.add(object);
            }

            RealTimeControlDataEntity disasterArea = new RealTimeControlDataEntity();
            disasterArea.setName(RealTimeControlTaskName.MONITOR_DISASTER_SPREAD);
            disasterArea.setValue(value);
            realTimeControlDAO.updateRealTimeControlDataByName(disasterArea);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private String matchWarningLevel(int level) {
        switch (level) {
            case 5: return "red";
            case 4: return "orange";
            case 3: return "yellow";
            case 2: return "blue";
            case 1: return "normal";
        }
        return "normal";
    }
}
