package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.MetroDataDAO;
import com.adc.disasterforecast.entity.MetroDataEntity;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.global.MetroTaskName;
import com.adc.disasterforecast.tools.DateHelper;
import com.adc.disasterforecast.tools.HttpHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

//@Component
public class MetroTask {
    // logger for MetroTask
    private static final Logger logger = LoggerFactory.getLogger(MetroTask.class);

    private static Map<Integer, Connection> lineMapping = new HashMap<>();
    private static Map<Integer, Station> stationMapping = new HashMap<>();
    private static List<Integer> line2ConnectionOrder;
    private static List<Integer> line16ConnectionOrder;
    private static List<Integer> line2StationOrder;
    private static List<Integer> line16StationOrder;

    private static final int noneLevel = 0;
    private static final int blueLevel = 1;
    private static final int yellowLevel = 2;
    private static final int orangeLevel = 3;
    private static final int redLevel = 4;
    static {
        line2ConnectionOrder = Arrays.asList(
                19, 18, 17, 16, 15, 14, 13, 20
        );
        line16ConnectionOrder = Arrays.asList(
                1, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2
        );
        line2StationOrder = Arrays.asList(
                22, 21, 20, 19, 18, 17, 16, 15, 14
        );
        line16StationOrder = Arrays.asList(
                13, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 1
        );
        lineMapping.put(1, new Connection("华夏中路-龙阳路", "16"));
        lineMapping.put(2, new Connection("滴水湖-临港大道", "16"));
        lineMapping.put(3, new Connection("临港大道-书院", "16"));
        lineMapping.put(4, new Connection("书院-惠南东", "16"));
        lineMapping.put(5, new Connection("惠南东-惠南", "16"));
        lineMapping.put(6, new Connection("惠南-野生动物园", "16"));
        lineMapping.put(7, new Connection("野生动物园-新场", "16"));
        lineMapping.put(8, new Connection("新场-航头东", "16"));
        lineMapping.put(9, new Connection("航头东-鹤沙航城", "16"));
        lineMapping.put(10, new Connection("鹤沙航城-周浦东", "16"));
        lineMapping.put(11, new Connection("周浦东-罗山路", "16"));
        lineMapping.put(12, new Connection("罗山路-华夏中路", "16"));

        lineMapping.put(13, new Connection("海天三路-远东大道", "2"));
        lineMapping.put(14, new Connection("远东大道-凌空路", "2"));
        lineMapping.put(15, new Connection("凌空路-川沙", "2"));
        lineMapping.put(16, new Connection("川沙-华夏东路", "2"));
        lineMapping.put(17, new Connection("华夏东路-创新中路", "2"));
        lineMapping.put(18, new Connection("创新中路-唐镇", "2"));
        lineMapping.put(19, new Connection("唐镇-广兰路", "2"));
        lineMapping.put(20, new Connection("浦东国际机场-海天三路", "2"));

        stationMapping.put(13, new Station("龙阳路", "16"));
        stationMapping.put(2, new Station("华夏中路", "16"));
        stationMapping.put(3, new Station("罗山路", "16"));
        stationMapping.put(4, new Station("周浦东", "16"));
        stationMapping.put(5, new Station("鹤沙航城", "16"));
        stationMapping.put(6, new Station("航头东", "16"));
        stationMapping.put(7, new Station("新场", "16"));
        stationMapping.put(8, new Station("野生动物", "16"));
        stationMapping.put(9, new Station("惠南", "16"));
        stationMapping.put(10, new Station("惠南东", "16"));
        stationMapping.put(12, new Station("临港大道", "16"));
        stationMapping.put(11, new Station("书院", "16"));
        stationMapping.put(1, new Station("滴水湖", "16"));

        stationMapping.put(14, new Station("浦东国际", "2"));
        stationMapping.put(15, new Station("海天三路", "2"));
        stationMapping.put(16, new Station("远东大道", "2"));
        stationMapping.put(18, new Station("川沙", "2"));
        stationMapping.put(19, new Station("华夏东路", "2"));
        stationMapping.put(20, new Station("创新中路", "2"));
        stationMapping.put(21, new Station("唐镇", "2"));
        stationMapping.put(22, new Station("广兰路", "2"));
        stationMapping.put(17, new Station("凌空路", "2"));
    }

    @Autowired
    private MetroDataDAO metroDataDAO;

    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetchGlobalWarning() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WARNING_FORCAST));

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WARNING_FORCAST);
            JSONArray value = new JSONArray();
            metroDataEntity.setValue(value);

            String url = JsonServiceURL.ALARM_JSON_SERVICE_URL
                    + "GetWeatherWarnning";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");

            int warningLevel = 0;

            for (Object o : array) {
                JSONObject input = (JSONObject) o;
                JSONObject output = new JSONObject();
                if (input.get("TYPE").equals("大风")) {
                    output.put("type", "wind");
                    output.put("des", input.get("CONTENT"));
                    if (input.get("LEVEL").equals("红色")) {
                        output.put("level", "red");
                        warningLevel = Math.max(warningLevel, redLevel);
                    } else if (input.get("LEVEL").equals("橙色")) {
                        output.put("level", "orange");
                        warningLevel = Math.max(warningLevel, orangeLevel);
                    } else if (input.get("LEVEL").equals("黄色")) {
                        output.put("level", "yellow");
                        warningLevel = Math.max(warningLevel, yellowLevel);
                    }
                    array.add(output);
                } else if (input.get("TYPE").equals("台风")) {
                    output.put("type", "typhoon");
                    output.put("des", input.get("CONTENT"));
                    if (input.get("LEVEL").equals("红色")) {
                        output.put("level", "red");
                        warningLevel = Math.max(warningLevel, redLevel);
                    } else if (input.get("LEVEL").equals("橙色")) {
                        output.put("level", "orange");
                        warningLevel = Math.max(warningLevel, orangeLevel);
                    } else if (input.get("LEVEL").equals("黄色")) {
                        output.put("level", "yellow");
                        warningLevel = Math.max(warningLevel, yellowLevel);
                    }
                    array.add(output);
                }
            }

            metroDataDAO.updateMetroDataByName(metroDataEntity);

            generateLinkageResponse(warningLevel);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetchWeatherForecast() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WEATHER_FORCAST));

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WEATHER_FORCAST);
            JSONArray value = new JSONArray();
            metroDataEntity.setValue(value);
            JSONObject today = new JSONObject();
            value.add(today);

            String url = JsonServiceURL.FORECAST_JSON_SERVICE_URL + "Get10DayForecast";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");
            JSONObject todayJo = (JSONObject) array.get(0);

            today.put("date", DateHelper.getPostponeDateByDay(0));
            today.put("weather", todayJo.get("Day"));
            today.put("minTemp", Integer.parseInt((String) todayJo.get("LowTmp")));
            today.put("maxTemp", Integer.parseInt((String) todayJo.get("HighTmp")));
            today.put("wind", (String) todayJo.get("Wind") + (String) todayJo.get("WindLev"));

            metroDataDAO.updateMetroDataByName(metroDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchLineWindInfluence() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WIND_INFLUENCE));
            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL
                    + "GetLastestMetroLineWindAlarm";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");

            long publishDate = readPublishTime();

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WIND_INFLUENCE);
            JSONArray value = new JSONArray();
            metroDataEntity.setValue(value);
            JSONObject line2Jo = new JSONObject();
            JSONObject line16Jo = new JSONObject();
            value.add(line2Jo);
            value.add(line16Jo);
            line2Jo.put("line", "line2");
            line2Jo.put("time", publishDate);
            line16Jo.put("line", "line16");
            line16Jo.put("time", publishDate);
            JSONArray line2Stations = new JSONArray();
            JSONArray line16Stations = new JSONArray();
            line2Jo.put("station", line2Stations);
            line16Jo.put("station", line16Stations);
            Map<Integer, JSONObject> line16Map = new HashMap<>();
            Map<Integer, JSONObject> line2Map = new HashMap<>();

            Set<Integer> set = new HashSet<>();
            for (Object o : array) {
                JSONObject one = (JSONObject) o;
                int smcLineId = (int) (long)one.get("SMC_LINE_ID");
                if (set.contains(smcLineId))
                    continue;
                set.add(smcLineId);
                Connection connection = lineMapping.get(smcLineId);
                JSONObject outputJo = new JSONObject();
                outputJo.put("section", connection.name);
                outputJo.put("level", one.get("LINECOLOR"));
                if (connection.line.equals("16")) line16Map.put(smcLineId, outputJo);
                else if (connection.line.equals("2")) line2Map.put(smcLineId, outputJo);
            }

            for (int connectionId : line16ConnectionOrder) {
                line16Stations.add(line16Map.get(connectionId));
            }
            for (int connectionId : line2ConnectionOrder) {
                line2Stations.add(line2Map.get(connectionId));
            }
            metroDataDAO.updateMetroDataByName(metroDataEntity);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private long readPublishTime() throws ParseException {
        String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL
                + "GetLastestMetroStationForecast";
        JSONObject jo = HttpHelper.getDataByURL(url);
        JSONArray array = (JSONArray) jo.get("Data");
        JSONObject jo1 = (JSONObject) array.get(0);
        String publishTime = (String) jo1.get("PUBLIST_TIME");
        return new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(publishTime).getTime();
    }

    @PostConstruct
    @Scheduled(cron = "0 0/10 * * * ?")
    public void fetchLineWindMonitor() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WIND_MONITOR));

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WIND_MONITOR);
            JSONArray value = new JSONArray();
            metroDataEntity.setValue(value);
            JSONObject line2Jo = new JSONObject();
            JSONObject line16Jo = new JSONObject();
            value.add(line2Jo);
            value.add(line16Jo);
            line2Jo.put("line", "line2");
            line16Jo.put("line", "line16");
            JSONArray line2Stations = new JSONArray();
            JSONArray line16Stations = new JSONArray();
            line2Jo.put("station", line2Stations);
            line16Jo.put("station", line16Stations);

            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL
                    + "GetLastestMetroStationForecast";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");

            Set<Integer> set = new HashSet<>();
            Map<Integer, JSONObject> line16Map = new HashMap<>();
            Map<Integer, JSONObject> line2Map = new HashMap<>();
            for (Object o : array) {
                JSONObject one = (JSONObject) o;
                int stationId = (int) (long)one.get("STATIONID");
                String forecastTime = (String)one.get("FORECAST_TIME");
                if (set.contains(stationId))
                    continue;
                set.add(stationId);
                Station station = stationMapping.get(stationId);
                JSONObject outputJo = new JSONObject();
                outputJo.put("site", station.name);
                outputJo.put("value", one.get("WINDSPEED"));
                outputJo.put("timestamp", forecastTime);
                int windSpeedClass = Integer.parseInt((String) one.get("WINDSPEED_CLASS"));
                String level;
                if (windSpeedClass == 4) level = "red";
                else if (windSpeedClass == 3) level = "orange";
                else if (windSpeedClass == 2) level = "yellow";
                else level = "green";
                outputJo.put("level", level);
                if (station.line.equals("16")) line16Map.put(stationId, outputJo);
                else if (station.line.equals("2")) line2Map.put(stationId, outputJo);
            }

            for (int stationId : line16StationOrder) {
                line16Stations.add(line16Map.get(stationId));
            }
            for (int stationId : line2StationOrder) {
                line2Stations.add(line2Map.get(stationId));
            }

            metroDataDAO.updateMetroDataByName(metroDataEntity);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    public void generateLinkageResponse(int warningLevel) {
        String responseLevel = "", carriageStatus = "", peopleStream = "";
        if (warningLevel == blueLevel) {
            responseLevel = "IV级应急响应，地铁大风风险一般";
            carriageStatus = "建议加强监测，做好地面、高架区段安全行车条件的检查和人流监控";
            peopleStream = "关注人流变化";
        } else if (warningLevel == yellowLevel) {
            responseLevel = "III级应急响应，地铁大风风险较重";
            carriageStatus = "对应的高架区段限速40km/h、地面区段限速，在高架与地面区段运行中的列车停车改为手动驾驶模式";
            peopleStream = "建议启动大客流响应预警III级。加强高价区段站、地面区段站的在站点出入口、通道内滞的现象，要及时疏导；同时做好信息发布";
        } else if (warningLevel == orangeLevel) {
            responseLevel = "II级应急响应，地铁大风风险严重";
            carriageStatus = "对应的高架区段限速40km/h、地面区段限速，在高架与地面区段运行中的列车停车改为手动驾驶模式";
            peopleStream = "建议启动大客流响应预警II级。加强高价区段站、地面区段站的在站点出入口、通道内滞的现象，要及时疏导；同时做好信息发布";
        } else if (warningLevel == redLevel) {
            responseLevel = "I级应急响应，地铁大风风险非常严重";
            carriageStatus = "对应的高架、地面区段列车暂停行驶；在高架与地面区段运行中的列车停车改为手动驾驶模式，以限速运行至就近车站清客，暂停行驶";
            peopleStream = "建议启动大客流响应预警II级+公交应急配套（短驳）。加强交路变更后的车站现场行车、客运组织工作，维保中心加强重点行车设备的保驾力量组织";
        }

        MetroDataEntity metroDataEntity = new MetroDataEntity();
        metroDataEntity.setName(MetroTaskName.GDJT_LINKAGE_RESPONSE);
        JSONArray value = new JSONArray();
        metroDataEntity.setValue(value);

        JSONObject jo = new JSONObject();
        jo.put("responseLevel", responseLevel);
        jo.put("cheliangyunxing", carriageStatus);
        jo.put("renliuchuli", peopleStream);
        value.add(jo);

        metroDataDAO.updateMetroDataByName(metroDataEntity);
    }

    private static class Connection {
        String name;
        String line;
        public Connection(String name, String line) {
            this.name = name;
            this.line = line;
        }
    }
    private static class Station {
        String name;
        String line;
        double longt;
        double latit;
        public Station(String name, String line) {
            this.name = name;
            this.line = line;
        }

        public Station(String name, String line, double longt, double latit) {
            this.name = name;
            this.line = line;
            this.longt = longt;
            this.latit = latit;
        }
    }
}
