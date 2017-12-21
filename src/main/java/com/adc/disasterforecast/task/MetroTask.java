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
import java.util.HashMap;
import java.util.Map;

@Component
public class MetroTask {
    // logger for MetroTask
    private static final Logger logger = LoggerFactory.getLogger(MetroTask.class);

    private static Map<Integer, Connection> lineMapping = new HashMap<>();
    private static Map<Integer, Station> stationMapping = new HashMap<>();
    static {
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

        stationMapping.put(13, new Station("龙阳路", "16", 121.552666, 31.204983));
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

            for (Object o : array) {
                JSONObject input = (JSONObject) o;
                JSONObject output = new JSONObject();
                if (input.get("TYPE").equals("大风")) {
                    output.put("type", "wind");
                    output.put("des", input.get("CONTENT"));
                    if (input.get("LEVEL").equals("红色")) output.put("level", "red");
                    else if (input.get("LEVEL").equals("橙色")) output.put("level", "orange");
                    else if (input.get("LEVEL").equals("黄色")) output.put("level", "yellow");
                    array.add(output);
                } else if (input.get("TYPE").equals("台风")) {
                    output.put("type", "typhoon");
                    output.put("des", input.get("CONTENT"));
                    if (input.get("LEVEL").equals("红色")) output.put("level", "red");
                    else if (input.get("LEVEL").equals("橙色")) output.put("level", "orange");
                    else if (input.get("LEVEL").equals("黄色")) output.put("level", "yellow");
                    array.add(output);
                }
            }

            metroDataDAO.updateMetroDataByName(metroDataEntity);
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
    public void fetchLineWindInflunce() {
        try {
            logger.info(String.format("began task：%s", MetroTaskName.GDJT_WIND_INFLUENCE));
            String url = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL
                    + "GetLastestMetroLineWindAlarm";
            JSONObject jo = HttpHelper.getDataByURL(url);
            JSONArray array = (JSONArray) jo.get("Data");

            MetroDataEntity metroDataEntity = new MetroDataEntity();
            metroDataEntity.setName(MetroTaskName.GDJT_WIND_INFLUENCE);
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

            for (Object o : array) {
                JSONObject one = (JSONObject) o;
                int smcLineId = (int) (long)one.get("SMC_LINE_ID");
                Connection connection = lineMapping.get(smcLineId);
                JSONObject outputJo = new JSONObject();
                outputJo.put("section", connection.name);
                outputJo.put("level", one.get("LINECOLOR"));
                if (connection.line.equals("16")) line16Stations.add(outputJo);
                else if (connection.line.equals("2")) line2Stations.add(outputJo);
            }
            metroDataDAO.updateMetroDataByName(metroDataEntity);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
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

            for (Object o : array) {
                JSONObject one = (JSONObject) o;
                int stationId = (int) (long) one.get("STATIONID");

            }

            metroDataDAO.updateMetroDataByName(metroDataEntity);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
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
        public Station(String name, String line, long longt, long latit) {
            this.name = name;
            this.line = line;
            this.longt = longt;
            this.latit = latit;
        }
    }
}
