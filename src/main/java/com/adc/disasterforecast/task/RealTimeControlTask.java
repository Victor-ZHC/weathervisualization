package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.RealTimeControlDAO;
import com.adc.disasterforecast.entity.RealTimeControlDataEntity;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.RealTimeControlTaskName;
import com.adc.disasterforecast.tools.*;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

@Component
public class RealTimeControlTask {
    // logger for RealTimeControlTask
    private static final Logger logger = LoggerFactory.getLogger(RealTimeControlTask.class);

    // dao Autowired
    @Autowired
    private RealTimeControlDAO realTimeControlDAO;

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countWeatherLive() {
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
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countRainfallAndMonitorAndWind() {
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
                if (rainfallValueNum < 0) {
                    rainfallValueNum = 0;
                }
                if (rainfallValueNum > 0) {
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
                if (windSpeedValueNum < 0) {
                    windSpeedValueNum = 0;
                }
                if (windSpeedValueNum > 0) {
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
            return 1;
        } else {
            return 0;
        }
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countThunderLive() {
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
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countMonitorStatsLive() {
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
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countThunderStatsLive() {
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

            JSONArray location = new JSONArray();
            thunderData.put("location", location);

            thunderData.put("level", "normal");

            thunderStatsLiveValue.add(thunderData);
        }

        RealTimeControlDataEntity thunderStatsLive = new RealTimeControlDataEntity();
        thunderStatsLive.setName(RealTimeControlTaskName.THUNDER_STATS_LIVE);
        thunderStatsLive.setValue(thunderStatsLiveValue);
        realTimeControlDAO.updateRealTimeControlDataByName(thunderStatsLive);
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countBroadcastV1() {
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
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countEarlyWarning() {
        logger.info(String.format("began task：%s", RealTimeControlTaskName.EARLY_WARNING));

        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";

        String beginDate = DateHelper.getCurrentTimeInString("day");
        String endDate = DateHelper.getCurrentTimeInString("minute");

        String url = baseUrl + beginDate + "/" + endDate;

        JSONObject obj = HttpHelper.getDataByURL(url);
        JSONArray earlyWarningDataArray = (JSONArray) obj.get("Data");

        JSONArray earlyWarningValue = new JSONArray();
        for (int i = 0; i < earlyWarningDataArray.size(); i++) {
            JSONObject earlyWarningDataObject = (JSONObject) earlyWarningDataArray.get(i);

            JSONObject warning = new JSONObject();
            warning.put("type", WarningHelper.getWarningWeather((String) earlyWarningDataObject.get("TYPE")));
            warning.put("warning", WarningHelper.getWarningWeather((String) earlyWarningDataObject.get("LEVEL")));
            earlyWarningValue.add(earlyWarningDataObject.get("CONTENT"));
        }

        RealTimeControlDataEntity earlyWarning = new RealTimeControlDataEntity();
        earlyWarning.setName(RealTimeControlTaskName.EARLY_WARNING);
        earlyWarning.setValue(earlyWarningValue);
        realTimeControlDAO.updateRealTimeControlDataByName(earlyWarning);
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countDisaster() {
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
    }

    private void addDisasterLive(JSONArray disasterArray) {

        int rain = 0;
        int wind = 0;
        int thunder = 0;

        for (int i = 0; i < disasterArray.size(); i++) {
            JSONObject disasterObject = (JSONObject) disasterArray.get(i);

            int disasterCode = ((Number) disasterObject.get("CODE_DISASTER")).intValue();

            switch (disasterCode) {
                case 1: {
                    rain++;
                    break;
                }
                case 2: {
                    wind++;
                    break;
                }
                case 3: {
                    thunder++;
                    break;
                }
                default: break;
            }
        }

        JSONObject disasterLiveObject = new JSONObject();
        disasterLiveObject.put("total", disasterArray.size());
        disasterLiveObject.put("rain", rain);
        disasterLiveObject.put("wind", wind);
        disasterLiveObject.put("thunder", thunder);
        JSONArray disasterLiveValue = new JSONArray();
        disasterLiveValue.add(disasterLiveObject);

        RealTimeControlDataEntity disasterLive = new RealTimeControlDataEntity();
        disasterLive.setName(RealTimeControlTaskName.DISASTER_LIVE);
        disasterLive.setValue(disasterLiveValue);
        realTimeControlDAO.updateRealTimeControlDataByName(disasterLive);
    }

    private void addDisasterArea(JSONArray disasterArray) {
        Map<String, Integer> disasterAreaMap = new HashMap<>();

        for (int i = 0; i < disasterArray.size(); i++) {
            JSONObject disasterObject = (JSONObject) disasterArray.get(i);

            String district = AreaHelper.getDistrictByCode(((Number) disasterObject.get("DISTRICT")).intValue());

            if (disasterAreaMap.containsKey(district)) {
                disasterAreaMap.put(district, disasterAreaMap.get(district) + 1);
            } else {
                disasterAreaMap.put(district, 1);
            }

        }

        JSONArray disasterAreaValue = new JSONArray();
        disasterAreaMap.forEach((k, v) -> {
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

        for (int i = 0; i < disasterArray.size(); i++) {
            JSONObject disasterObject = (JSONObject) disasterArray.get(i);

            long date =  DateHelper.getDateInLongByHour((String) disasterObject.get("DATETIME_DISASTER"));

            if (disasterTimeMap.containsKey(date)) {
                disasterTimeMap.put(date, disasterTimeMap.get(date) + 1);
            } else {
                disasterTimeMap.put(date, 1);
            }
        }

        JSONArray disasterTimeValue = new JSONArray();
        disasterTimeMap.forEach((k, v) -> {
            JSONObject disasterTimeObject = new JSONObject();
            disasterTimeObject.put("time", k);
            disasterTimeObject.put("value", v);
            disasterTimeValue.add(disasterTimeObject);
        });

        RealTimeControlDataEntity disasterTime = new RealTimeControlDataEntity();
        disasterTime.setName(RealTimeControlTaskName.DISASTER_TIME_PERIOD);
        disasterTime.setValue(disasterTimeValue);
        realTimeControlDAO.updateRealTimeControlDataByName(disasterTime);
    }

    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countHistoryWarning() {
        logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_WARNING));
        logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_WARNING_AVG));
        logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_WARNING_MONTH));

        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetWeatherWarnningByDatetime/";

        // 获取开始年份
        String beginTime = (Calendar.getInstance().get(Calendar.YEAR) - 10) + "0101000000";
        String endTime = DateHelper.getCurrentTimeInString("minute");

        String url = baseUrl + beginTime + "/" + endTime;

        JSONObject obj = HttpHelper.getDataByURL(url);
        JSONArray historyWarningArray = (JSONArray) obj.get("Data");

        addHistoryWarning(historyWarningArray);
        addHistoryWarningAvg(historyWarningArray, 10);
        addHistoryWarningMouth(historyWarningArray, 10);
    }

    private void addHistoryWarning(JSONArray historyWarningArray) {
        Map<Integer, Integer> historyWarningMap = new HashMap<>();
        for (int i = 0; i < historyWarningArray.size(); i++) {
            JSONObject historyWarning = (JSONObject) historyWarningArray.get(i);

            int warningYear = Integer.valueOf(DateHelper.getYear((String) historyWarning.get("FORECASTDATE")));

            if (historyWarningMap.containsKey(warningYear)) {
                historyWarningMap.put(warningYear, historyWarningMap.get(warningYear) + 1);
            } else {
                historyWarningMap.put(warningYear, 1);
            }
        }

        JSONArray historyWarningValue = new JSONArray();
        historyWarningMap.forEach((k, v) -> {
            JSONObject historyWarningObject = new JSONObject();
            historyWarningObject.put("time", k);
            historyWarningObject.put("value", v);
            historyWarningValue.add(historyWarningObject);
        });

        RealTimeControlDataEntity historyWarning = new RealTimeControlDataEntity();
        historyWarning.setName(RealTimeControlTaskName.HISTORY_WARNING);
        historyWarning.setValue(historyWarningValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyWarning);

    }

    private void addHistoryWarningAvg(JSONArray historyWarningArray, int total) {
        Map<String, Integer> historyWarningAvgMap = new HashMap<>();
        int totalNum = historyWarningArray.size();
        for (int i = 0; i < historyWarningArray.size(); i++) {
            JSONObject historyWarning = (JSONObject) historyWarningArray.get(i);

            String warningType = WarningHelper.getWarningWeather((String) historyWarning.get("TYPE"));

            if (historyWarningAvgMap.containsKey(warningType)) {
                historyWarningAvgMap.put(warningType, historyWarningAvgMap.get(warningType) + 1);
            } else {
                historyWarningAvgMap.put(warningType, 1);
            }
        }

        JSONArray historyWarningAvgValue = new JSONArray();
        JSONObject historyWarningAvgObject = new JSONObject();
        historyWarningAvgMap.forEach((k, v) -> historyWarningAvgObject.put(k, v / total));
        historyWarningAvgObject.put("total", totalNum / total);
        historyWarningAvgValue.add(historyWarningAvgObject);

        RealTimeControlDataEntity historyWarningAvg = new RealTimeControlDataEntity();
        historyWarningAvg.setName(RealTimeControlTaskName.HISTORY_WARNING_AVG);
        historyWarningAvg.setValue(historyWarningAvgValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyWarningAvg);
    }

    private void addHistoryWarningMouth(JSONArray historyWarningArray, int total) {
        Map<String, Integer> historyWarningMouthMap = new HashMap<>();
        int totalNum = 0;
        for (int i = 0; i < historyWarningArray.size(); i++) {
            JSONObject historyWarning = (JSONObject) historyWarningArray.get(i);

            if (String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1).equals(DateHelper.getMonth((String) historyWarning.get("FORECASTDATE")))) {
                totalNum++;
                String warningType = WarningHelper.getWarningWeather((String) historyWarning.get("TYPE"));

                if (historyWarningMouthMap.containsKey(warningType)) {
                    historyWarningMouthMap.put(warningType, historyWarningMouthMap.get(warningType) + 1);
                } else {
                    historyWarningMouthMap.put(warningType, 1);
                }
            }
        }

        JSONArray historyWarningMonthValue = new JSONArray();
        JSONObject historyWarningMonthObject = new JSONObject();
        historyWarningMouthMap.forEach((k, v) -> historyWarningMonthObject.put(k, v / total));
        historyWarningMonthObject.put("total", totalNum / total);
        historyWarningMonthValue.add(historyWarningMonthObject);

        RealTimeControlDataEntity historyWarningMonth = new RealTimeControlDataEntity();
        historyWarningMonth.setName(RealTimeControlTaskName.HISTORY_WARNING_MONTH);
        historyWarningMonth.setValue(historyWarningMonthValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyWarningMonth);
    }


    //@PostConstruct
    @Scheduled(cron = "* */10 * * * *")
    public void countHistoryDisaster() {
        logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_DISASTER));
        logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_DISASTER_AVG));
        logger.info(String.format("began task：%s", RealTimeControlTaskName.HISTORY_DISASTER_MONTH));

        String baseUrl = JsonServiceURL.ALARM_JSON_SERVICE_URL + "GetRealDisasterDetailData_Geliku/";

        // 获取开始年份
        String beginTime = (Calendar.getInstance().get(Calendar.YEAR) - 10) + "0101000000";
        String endTime = DateHelper.getCurrentTimeInString("minute");

        String url = baseUrl + beginTime + "/" + endTime;

        JSONObject obj = HttpHelper.getDataByURL(url);
        JSONArray historyDisasterArray = (JSONArray) obj.get("Data");

        addHistoryDisaster(historyDisasterArray);
        addHistoryDisasterAvg(historyDisasterArray, 10);
        addHistoryDisasterMouth(historyDisasterArray, 10);
    }

    private void addHistoryDisaster(JSONArray historyDisasterArray) {
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

        JSONArray historyDisasterValue = new JSONArray();
        historyWarningMap.forEach((k, v) -> {
            JSONObject historyDisasterObject = new JSONObject();
            historyDisasterObject.put("time", k);
            historyDisasterObject.put("value", v);
            historyDisasterValue.add(historyDisasterObject);
        });

        RealTimeControlDataEntity historyDisaster = new RealTimeControlDataEntity();
        historyDisaster.setName(RealTimeControlTaskName.HISTORY_DISASTER);
        historyDisaster.setValue(historyDisasterValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyDisaster);

    }

    private void addHistoryDisasterAvg(JSONArray historyDisasterArray, int total) {
        Map<String, Integer> historyDisasterAvgMap = new HashMap<>();
        int totalNum = historyDisasterArray.size();
        for (int i = 0; i < historyDisasterArray.size(); i++) {
            JSONObject historyDisaster = (JSONObject) historyDisasterArray.get(i);

            String disasterType = DisasterTypeHelper.getDisasterTypeByCode(((Number) historyDisaster.get("CODE_DISASTER")).intValue());

            if (historyDisasterAvgMap.containsKey(disasterType)) {
                historyDisasterAvgMap.put(disasterType, historyDisasterAvgMap.get(disasterType) + 1);
            } else {
                historyDisasterAvgMap.put(disasterType, 1);
            }
        }

        JSONArray historyDisasterAvgValue = new JSONArray();
        JSONObject historyDisasterAvgObject = new JSONObject();
        historyDisasterAvgMap.forEach((k, v) -> historyDisasterAvgObject.put(k, v / total));
        historyDisasterAvgObject.put("total", totalNum / total);
        historyDisasterAvgValue.add(historyDisasterAvgObject);

        RealTimeControlDataEntity historyDisasterAvg = new RealTimeControlDataEntity();
        historyDisasterAvg.setName(RealTimeControlTaskName.HISTORY_DISASTER_AVG);
        historyDisasterAvg.setValue(historyDisasterAvgValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyDisasterAvg);
    }

    private void addHistoryDisasterMouth(JSONArray historyDisasterArray, int total) {
        Map<String, Integer> historyDisasterMouthMap = new HashMap<>();
        int totalNum = 0;
        for (int i = 0; i < historyDisasterArray.size(); i++) {
            JSONObject historyDisaster = (JSONObject) historyDisasterArray.get(i);

            if (String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1).equals(DateHelper.getMonth((String) historyDisaster.get("DATETIME_DISASTER")))) {
                totalNum++;
                String disasterType = DisasterTypeHelper.getDisasterTypeByCode(((Number) historyDisaster.get("CODE_DISASTER")).intValue());

                if (historyDisasterMouthMap.containsKey(disasterType)) {
                    historyDisasterMouthMap.put(disasterType, historyDisasterMouthMap.get(disasterType) + 1);
                } else {
                    historyDisasterMouthMap.put(disasterType, 1);
                }
            }
        }

        JSONArray historyDisasterMonthValue = new JSONArray();
        JSONObject historyDisasterMonthObject = new JSONObject();
        historyDisasterMouthMap.forEach((k, v) -> historyDisasterMonthObject.put(k, v / total));
        historyDisasterMonthObject.put("total", totalNum / total);
        historyDisasterMonthValue.add(historyDisasterMonthObject);

        RealTimeControlDataEntity historyDisasterMonth = new RealTimeControlDataEntity();
        historyDisasterMonth.setName(RealTimeControlTaskName.HISTORY_DISASTER_MONTH);
        historyDisasterMonth.setValue(historyDisasterMonthValue);
        realTimeControlDAO.updateRealTimeControlDataByName(historyDisasterMonth);
    }
}
