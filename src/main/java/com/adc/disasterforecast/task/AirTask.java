package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.AirDataDAO;
import com.adc.disasterforecast.entity.AirDataEntity;
import com.adc.disasterforecast.global.AirTaskName;
import com.adc.disasterforecast.global.JsonServiceURL;
import com.adc.disasterforecast.tools.HttpHelper;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

//@Component
public class AirTask {
    // logger for AirTask
    private static final Logger logger = LoggerFactory.getLogger(AirTask.class);
    private static final String PUDONG_AIRPORT_ICAO = "ZSPD";
    private static final String HONGQIAO_AIRPORT_ICAO = "ZSSS";

    @Autowired
    private AirDataDAO airDataDAO;


    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetchAirportCapacity() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_AIRPORT_CAPACTIY));
            logger.info(String.format("began task：%s", AirTaskName.HKQX_AIRPORT_CAPACTIY_DAY));
            logger.info(String.format("began task：%s", AirTaskName.HKQX_WEATHER_INFLUENCE));
            logger.info(String.format("began task：%s", AirTaskName.HKQX_WARNING));
            String pudongUrl = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL
                    + "GetLastestAirPortForecastByICAO/"
                    + PUDONG_AIRPORT_ICAO;
            JSONObject pudongJo = HttpHelper.getDataByURL(pudongUrl);
            JSONArray pudongArray = (JSONArray) pudongJo.get("Data");

            String hongqiaoUrl = JsonServiceURL.METEOROLOGICAL_JSON_SERVICE_URL
                    + "GetLastestAirPortForecastByICAO/"
                    + HONGQIAO_AIRPORT_ICAO;
            JSONObject hongqiaoJo = HttpHelper.getDataByURL(hongqiaoUrl);
            JSONArray hongqiaoArray = (JSONArray) hongqiaoJo.get("Data");

            generateAirportCapacity(pudongArray, hongqiaoArray);
            generateAirportCapacityDay(pudongArray, hongqiaoArray);
            generateAirportWeatherInfluence(pudongArray, hongqiaoArray);
            generateAirportWarning(pudongArray, hongqiaoArray);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillCloudData() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_INFLUENCE_CLOUD));
            List<Double> hongqiaoData= Arrays.asList(
                    0.9, 1.3, 0.8, 0.5, 0.3, 0.2,
                    0.2, 0.0, 0.1, 0.1, 0.1, 0.5);
            double hongqiaoAvg = 5.0;
            List<Double> pudongData= Arrays.asList(
                    0.1, 3.0, 1.7, 2.1, 1.2, 1.4,
                    0.0, 0.0, 0.0, 0.0, 0.3, 0.0);
            double pudongAvg = 9.8;
            fillDisasterData(AirTaskName.HKQX_INFLUENCE_CLOUD, hongqiaoData, hongqiaoAvg, pudongData, pudongAvg);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillFogData() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_INFLUENCE_FOG));
            List<Double> hongqiaoData= Arrays.asList(
                    2.7, 2.4, 1.5, 1.5, 0.5, 0.9,
                    0.1, 0.1, 0.1, 1.3, 3.4, 3.4);
            double hongqiaoAvg = 17.9;
            List<Double> pudongData= Arrays.asList(
                    2.7, 4.5, 3.0, 3.6, 2.3, 2.1,
                    0.2, 0.3, 0.1, 0.4, 3.0, 1.7);
            double pudongAvg = 23.9;
            fillDisasterData(AirTaskName.HKQX_INFLUENCE_FOG, hongqiaoData, hongqiaoAvg, pudongData, pudongAvg);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillWindData() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_INFLUENCE_WIND));
            List<Double> hongqiaoData= Arrays.asList(
                    0.4, 0.1, 1.1, 0.8, 0.5, 0.6,
                    2.1, 2.1, 0.8, 0.3, 0.6, 0.7);
            double hongqiaoAvg = 10.1;
            List<Double> pudongData= Arrays.asList(
                    0.8, 1.0, 2.1, 1.6, 1.5, 1.0,
                    2.4, 2.7, 1.7, 1.3, 1.3, 2.1);
            double pudongAvg = 19.5;
            fillDisasterData(AirTaskName.HKQX_INFLUENCE_WIND, hongqiaoData, hongqiaoAvg, pudongData, pudongAvg);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillThunderData() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_INFLUENCE_THUNDER));
            List<Double> hongqiaoData= Arrays.asList(
                    0.1, 0.9, 1.6, 2.1, 1.1, 3.4,
                    7.3, 8.4, 2.4, 0.3, 0.4, 0.1);
            double hongqiaoAvg = 28.1;
            List<Double> pudongData= Arrays.asList(
                    0.0, 0.8, 1.5, 1.9, 1.1, 3.1,
                    5.9, 6.9, 2.1, 0.3, 0.3, 0.1);
            double pudongAvg = 24.0;
            fillDisasterData(AirTaskName.HKQX_INFLUENCE_THUNDER, hongqiaoData, hongqiaoAvg, pudongData, pudongAvg);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillDisasterWeather() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_DISASTER_TYPE));

            AirDataEntity airDataEntity = new AirDataEntity();
            airDataEntity.setName(AirTaskName.HKQX_DISASTER_TYPE);
            JSONArray value = new JSONArray();
            airDataEntity.setValue(value);

            double wind = 10.1 + 19.5;
            double thunder = 28.1 + 24.0;
            double fog = 17.9 + 23.9;
            double cloud = 5.0 + 9.8;
            JSONObject windJo = new JSONObject();
            windJo.put("type", "大风");
            windJo.put("value", wind);
            value.add(windJo);
            JSONObject thunderJo = new JSONObject();
            thunderJo.put("type", "雷暴");
            thunderJo.put("value", thunder);
            value.add(thunderJo);
            JSONObject fogJo = new JSONObject();
            fogJo.put("type", "低能见度");
            fogJo.put("value", fog);
            value.add(fogJo);
            JSONObject cloudJo = new JSONObject();
            cloudJo.put("type", "低云");
            cloudJo.put("value", cloud);
            value.add(cloudJo);

            airDataDAO.updateAirDataByName(airDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillWarningYear() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_WARNING_YEAR));
            AirDataEntity airDataEntity = new AirDataEntity();
            airDataEntity.setName(AirTaskName.HKQX_WARNING_YEAR);
            JSONArray value = new JSONArray();
            airDataEntity.setValue(value);
            JSONObject jo = new JSONObject();
            value.add(jo);
            jo.put("channel", "网页、短息等方式");
            jo.put("service", "空管部门、机场");
            airDataDAO.updateAirDataByName(airDataEntity);

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    @PostConstruct
    public void fillExposity() {
        try {
            logger.info(String.format("began task：%s", AirTaskName.HKQX_AIRPORT_EXPOSE));
            AirDataEntity airDataEntity = new AirDataEntity();
            airDataEntity.setName(AirTaskName.HKQX_AIRPORT_EXPOSE);
            JSONArray value = new JSONArray();
            airDataEntity.setValue(value);
            JSONObject jo = new JSONObject();
            value.add(jo);

            List<Double> pudongData = Arrays.asList(
                    0.575, 0.15, 0.0875, 0.025,
                    0.1375, 0.1, 0.15, 0.2,
                    0.7125, 0.875, 0.8375, 0.925,
                    0.8875, 0.95, 0.85, 0.8875,
                    0.8625, 0.9, 0.9, 0.8375,
                    0.85, 0.825, 0.8625, 0.7
            );

            List<Double> hongqiaoData = Arrays.asList(
                    0.68, 0.0, 0.0, 0.0,
                    0.02, 0.0, 0.0, 0.2,
                    0.72, 0.76, 0.84, 0.88,
                    1.0, 1.02, 0.96, 0.94,
                    0.9, 0.96, 0.94, 0.94,
                    0.9, 0.92, 0.7, 0.7
            );

            jo.put("pudong", generateExposity(pudongData));
            jo.put("hongqiao", generateExposity(hongqiaoData));

            airDataDAO.updateAirDataByName(airDataEntity);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void fillDisasterData(String apiName,
                                  List<Double> hongqiaoData, double hongqiaoAvg,
                                  List<Double> pudongData, double pudongAvg) {
        AirDataEntity airDataEntity = new AirDataEntity();
        airDataEntity.setName(apiName);
        JSONArray value = new JSONArray();
        airDataEntity.setValue(value);
        JSONObject jo = new JSONObject();
        value.add(jo);
        JSONObject pudongJo = new JSONObject();
        JSONObject hongqiaoJo = new JSONObject();
        jo.put("pudong", pudongJo);
        jo.put("hongqiao", hongqiaoJo);

        pudongJo.put("avg", pudongAvg);
        JSONArray pudongMonth = new JSONArray();
        pudongJo.put("month", pudongMonth);
        for (int i = 0; i < pudongData.size(); ++i) {
           JSONObject one = new JSONObject();
           one.put("date", i + 1);
           one.put("value", pudongData.get(i));
           pudongMonth.add(one);
        }

        hongqiaoJo.put("avg", hongqiaoAvg);
        JSONArray hongqiaoMonth = new JSONArray();
        hongqiaoJo.put("month", hongqiaoMonth);
        for (int i = 0; i < hongqiaoData.size(); ++i) {
            JSONObject one = new JSONObject();
            one.put("date", i + 1);
            one.put("value", hongqiaoData.get(i));
            hongqiaoMonth.add(one);
        }

        airDataDAO.updateAirDataByName(airDataEntity);
    }

    private void generateAirportCapacityDay(JSONArray pudongArray, JSONArray hongqiaoArray) throws ParseException {
        AirDataEntity airDataEntity = new AirDataEntity();
        airDataEntity.setName(AirTaskName.HKQX_AIRPORT_CAPACTIY_DAY);
        JSONArray value = new JSONArray();
        airDataEntity.setValue(value);
        JSONObject jo = new JSONObject();
        value.add(jo);
        JSONArray pudongJa = new JSONArray();
        JSONArray hongqiaoJa = new JSONArray();
        jo.put("pudong", pudongJa);
        jo.put("hongqiao", hongqiaoJa);

        updateAirportCapacity(pudongArray, pudongJa);
        updateAirportCapacity(hongqiaoArray, hongqiaoJa);

        airDataDAO.updateAirDataByName(airDataEntity);
    }

    private void updateAirportCapacity(JSONArray inputArray, JSONArray outputArray) throws ParseException {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss");
        for (Object o : inputArray) {
            JSONObject one = (JSONObject) o;
            JSONObject data = new JSONObject();

            data.put("date", df.parse((String) one.get("FORECAST_TIME")).getTime());
            data.put("value", (int) ((double) one.get("IMPACT_INDEX") * 100));

            outputArray.add(data);
        }
    }

    private void generateAirportCapacity(JSONArray pudongArray, JSONArray hongqiaoArray) {
        AirDataEntity airDataEntity = new AirDataEntity();
        airDataEntity.setName(AirTaskName.HKQX_AIRPORT_CAPACTIY);
        JSONArray value = new JSONArray();
        airDataEntity.setValue(value);
        JSONObject jo = new JSONObject();
        value.add(jo);
        JSONObject pudongJo = new JSONObject();
        JSONObject hongqiaoJo = new JSONObject();
        jo.put("pudong", pudongJo);
        jo.put("hongqiao", hongqiaoJo);

        JSONObject pudongIndexObj = (JSONObject) pudongArray.stream().max(Comparator.comparing(
                o -> (double)((JSONObject) o).get("IMPACT_INDEX")
        )).get();
        double pudongIndex = (double) pudongIndexObj.get("IMPACT_INDEX");
        JSONObject hongqiaoIndexObj = (JSONObject) hongqiaoArray.stream().max(Comparator.comparing(
                o -> (double)((JSONObject) o).get("IMPACT_INDEX")
        )).get();
        double hongqiaoIndex = (double) hongqiaoIndexObj.get("IMPACT_INDEX");

        pudongJo.put("value", (int)(pudongIndex * 100));
        pudongJo.put("level", capacityLevel(pudongIndex));

        hongqiaoJo.put("value", (int)(hongqiaoIndex * 100));
        hongqiaoJo.put("level", capacityLevel(hongqiaoIndex));

        airDataDAO.updateAirDataByName(airDataEntity);
    }

    private int capacityLevel(double value) {
        if (value < 0.25) return 4;
        if (value < 0.5) return 3;
        if (value < 0.75) return 2;
        return 1;
    }

    private void generateAirportWeatherInfluence(JSONArray pudongArray, JSONArray hongqiaoArray) {
        AirDataEntity airDataEntity = new AirDataEntity();
        airDataEntity.setName(AirTaskName.HKQX_WEATHER_INFLUENCE);
        JSONArray value = new JSONArray();
        airDataEntity.setValue(value);
        JSONObject jo = new JSONObject();
        value.add(jo);
        JSONArray pudongJa = new JSONArray();
        JSONArray hongqiaoJa = new JSONArray();
        jo.put("pudong", pudongJa);
        jo.put("hongqiao", hongqiaoJa);

        updateAirportWeatherInfluence(pudongArray, pudongJa);
        updateAirportWeatherInfluence(hongqiaoArray, hongqiaoJa);

        airDataDAO.updateAirDataByName(airDataEntity);
    }

    private void updateAirportWeatherInfluence(JSONArray input, JSONArray output) {
        JSONObject cloudJo = (JSONObject) input.stream().min(Comparator.comparing(
                o -> (double)((JSONObject) o).get("CEIL_METER")
        )).get();
        JSONObject rainJo = (JSONObject) input.stream().max(Comparator.comparing(
                o -> (double)((JSONObject) o).get("RAIN_MM")
        )).get();
        JSONObject fogJo = (JSONObject) input.stream().min(Comparator.comparing(
                o -> (double)((JSONObject) o).get("VIS_METER")
        )).get();
        JSONObject thunderJo = (JSONObject) input.stream().max(Comparator.comparing(
                o -> (double)((JSONObject) o).get("TS")
        )).get();
        JSONObject windJo = (JSONObject) input.stream().max(Comparator.comparing(
                o -> (double)((JSONObject) o).get("WIND_SPEED")
        )).get();

        double cloud = (double) cloudJo.get("CEIL_METER");
        double rain = (double) rainJo.get("RAIN_MM");
        double fog = (double) fogJo.get("VIS_METER");
        double thunder = (double) thunderJo.get("TS");
        double wind = (double) windJo.get("WIND_SPEED");

        int cloudLevel;
        if (cloud < 65) cloudLevel = 1;
        else if (cloud < 85) cloudLevel = 2;
        else if (cloud < 100) cloudLevel = 3;
        else cloudLevel = 4;

        int rainLevel;
        if (rain > 15) rainLevel = 1;
        else if (rain > 10) rainLevel = 2;
        else if (rain > 5) rainLevel = 3;
        else rainLevel = 4;

        int fogLevel;
        if (fog < 600) fogLevel = 1;
        else if (fog < 800) fogLevel = 2;
        else if (fog < 1000) fogLevel = 3;
        else fogLevel = 4;

        int thunderLevel;
        if (thunder == 2) thunderLevel = 1;
        else if (thunder == 1) thunderLevel = 2;
        else thunderLevel = 4;

        int windLevel;
        if (wind >= 20) windLevel = 1;
        else if (wind >= 15) windLevel = 2;
        else if (wind >= 8) windLevel = 3;
        else windLevel = 4;

        JSONObject cloudOutput = new JSONObject();
        cloudOutput.put("type", "低云");
        cloudOutput.put("value", cloudLevel);
        output.add(cloudOutput);
        JSONObject rainOutput = new JSONObject();
        rainOutput.put("type", "强降水");
        rainOutput.put("value", rainLevel);
        output.add(rainOutput);
        JSONObject thunderOutput = new JSONObject();
        thunderOutput.put("type", "雷暴");
        thunderOutput.put("value", thunderLevel);
        output.add(thunderOutput);
        JSONObject windOutput = new JSONObject();
        windOutput.put("type", "大风");
        windOutput.put("value", windLevel);
        output.add(windOutput);
        JSONObject fogOutput = new JSONObject();
        fogOutput.put("type", "低能见度");
        fogOutput.put("value", fogLevel);
        output.add(fogOutput);
    }

    private JSONArray generateExposity(List<Double> data) {
        JSONArray array = new JSONArray();
        for (int i = 0; i < data.size(); ++i) {
            JSONObject jo = new JSONObject();
            jo.put("date", i + 1);
            jo.put("value", data.get(i));
            array.add(jo);
        }
        return array;
    }

    private void generateAirportWarning(JSONArray pudongArray, JSONArray hongqiaoArray) {
        AirDataEntity airDataEntity = new AirDataEntity();
        airDataEntity.setName(AirTaskName.HKQX_WARNING);
        JSONArray value = new JSONArray();
        airDataEntity.setValue(value);

        value.add(generateWarningByAirport("浦东", pudongArray));
        value.add(generateWarningByAirport("虹桥", hongqiaoArray));

        airDataDAO.updateAirDataByName(airDataEntity);
    }

    private String generateWarningByAirport(String airportName, JSONArray array) {
        JSONObject indexObj = (JSONObject) array.stream().max(Comparator.comparing(
                o -> (double)((JSONObject) o).get("IMPACT_INDEX")
        )).get();
        double index = (double) indexObj.get("IMPACT_INDEX");
        int newIndex = (int) (100 * index);
        int level = capacityLevel(index);
        String levelString;
        if (level == 4) levelString = "IV";
        else if (level == 3) levelString = "III";
        else if (level == 2) levelString = "II";
        else levelString = "I";

        return String.format("%s机场延误等级%s(%d%%)", airportName, levelString, newIndex);
    }
}
