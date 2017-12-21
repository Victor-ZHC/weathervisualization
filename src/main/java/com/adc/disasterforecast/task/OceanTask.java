package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.OceanDataDAO;
import com.adc.disasterforecast.entity.OceanDataEntity;
import com.adc.disasterforecast.global.OceanTaskName;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Random;

@Component
public class OceanTask {
    // logger for OceanTask
    private static final Logger logger = LoggerFactory.getLogger(OceanTask.class);

    @Autowired
    private OceanDataDAO oceanDataDAO;

    @PostConstruct
    public void fillAllData() {
        try {
            fillData(OceanTaskName.HYQX_COURSE_INFLUENCE, createKPI_HYQX_COURSE_INFLUENCE());
            fillData(OceanTaskName.HYQX_ABNORMAL_WAVE_MONTH, createKPI_HYQX_ABNORMAL_WAVE_MONTH());
            fillData(OceanTaskName.HYQX_SERVICE_PUBLISH, createKPI_HYQX_SERVICE_PUBLISH());
            fillData(OceanTaskName.HYQX_WARNING_TYPE, createKPI_HYQX_WARNING_TYPE());
            fillData(OceanTaskName.HYQX_WAVE_STEEPNESS, createKPI_HYQX_WAVE_STEEPNESS());
            fillData(OceanTaskName.HYQX_SHOAL_EFFECT, createKPI_HYQX_SHOAL_EFFECT());
            fillData(OceanTaskName.HYQX_SURGE_PROPORTION, createKPI_HYQX_SURGE_PROPORTION());
            fillData(OceanTaskName.HYQX_NAVIGATION_BROADCAST, createKPI_HYQX_NAVIGATION_BROADCAST());
            fillData(OceanTaskName.HYQX_SYNC_ROLLING, createKPI_HYQX_SYNC_ROLLING());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private JSONArray createKPI_HYQX_SYNC_ROLLING() {
        JSONArray array = new JSONArray();
//        double upperBound = 1.5, lowerBound = 0.1;
        List<Float> list1 = Arrays.asList(
                2.2f, 4.1f, 4.1f, 3.9f, 3.8f, 3.7f,
                3.6f, 3.8f, 3.7f, 3.7f, 3.8f, 3.6f,
                2.7f, 3.9f, 3.6f, 3.7f, 3.4f, 3f,
                1.2f, 1.2f, 1.7f, 1f, 1.4f, 1.7f
        );
        List<Float> list2 = Arrays.asList(
                3.2f, 3.1f, 3.0f, 3.9f, 2.9f, 3.3f,
                2.8f, 2.8f, 3.7f, 3.6f, 3.8f, 2.6f,
                2.2f, 2.7f, 2.8f, 2.9f, 3.1f, 3.2f,
                1f, 2.2f, 2.6f, 1.3f, 1.8f, 1.8f
        );
        List<Float> list3 = Arrays.asList(
                3.0f, 4.0f, 3.3f, 3.8f, 3.8f, 3f,
                2.9f, 2.8f, 2.7f, 2.8f, 3.8f, 2.6f,
                3f, 2.9f, 2.2f, 2.8f, 3.2f, 3f,
                2.9f, 2.2f, 2.5f, 1.9f, 2.4f, 1.7f
        );
        List<Float> list4 = Arrays.asList(
                3.2f, 2.1f, 2.1f, 2.9f, 2.8f, 3f,
                2.9f, 2.8f, 2.7f, 2.7f, 2.3f, 2.2f,
                1.3f, 1.9f, 1.8f, 1.7f, 1f, 1f,
                1.8f, 2.2f, 2.5f, 1.9f, 2.6f, 2.7f
        );
        array.add(createChart("A", list1));
        array.add(createChart("B", list2));
        array.add(createChart("C", list3));
        array.add(createChart("D", list4));
        return  array;
    }

    private JSONObject createRandomChart(String siteName, double upperBound, double lowerBound) {
        JSONObject jo = new JSONObject();
        jo.put("site", siteName);
        JSONArray value = new JSONArray();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -3);
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Random rand = new Random();
        for (int i = 0; i < 72; ++i) {
            JSONObject data = new JSONObject();
            data.put("date", cal.getTimeInMillis());
            data.put("value", rand.nextDouble() * (upperBound - lowerBound) + lowerBound);
            cal.add(Calendar.HOUR_OF_DAY, 1);
            value.add(data);
        }
        jo.put("value", value);
        return jo;
    }

    private JSONObject createRandomChart(String siteName, List<Integer> values) {
        JSONObject jo = new JSONObject();
        jo.put("site", siteName);
        JSONArray value = new JSONArray();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -3);
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Random rand = new Random();
        for (int i = 0; i < 72; ++i) {
            JSONObject data = new JSONObject();
            data.put("date", cal.getTimeInMillis());
            data.put("value", values.get(rand.nextInt(values.size())));
            cal.add(Calendar.HOUR_OF_DAY, 1);
            value.add(data);
        }
        jo.put("value", value);
        return jo;
    }

    private JSONObject createChart(String siteName, List<Float> values) {
        JSONObject jo = new JSONObject();
        jo.put("site", siteName);
        JSONArray value = new JSONArray();
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DAY_OF_YEAR, -1);
        cal.add(Calendar.HOUR_OF_DAY, -1);
        Random rand = new Random();
        for (int i = 0; i < 24; ++i) {
            JSONObject data = new JSONObject();
            data.put("date", cal.getTimeInMillis());
            data.put("value", values.get(i));
            cal.add(Calendar.HOUR_OF_DAY, 1);
            value.add(data);
        }
        jo.put("value", value);
        return jo;
    }

    private JSONArray createKPI_HYQX_NAVIGATION_BROADCAST() {
        JSONArray array = new JSONArray();
        array.add(createWarning("A", 1,
                "对应IV级应急响应，船舶航行风险一般，" +
                        "建议滑型船调整好航行策略，" +
                        "避免受横浪影响"));
        array.add(createWarning("B", 3,
                "对应II级应急响应，船舶航行风险严重，" +
                        "建议滑水型船进港避风，" +
                        "排水型船调整好航行策略，" +
                        "避免受横浪影响"));
        array.add(createWarning("C", 2,
                "对应III级应急响应，船舶航行风险中等，" +
                        "建议所有船舶调整好航行策略，" +
                        "避免受横浪影响"));
        array.add(createWarning("D", 1,
                "对应IV级应急响应，船舶航行风险一般，" +
                        "建议滑型船调整好航行策略，" +
                        "避免受横浪影响"));
        return array;
    }

    private JSONObject createWarning(String site, int level, String desc) {
        JSONObject jo = new JSONObject();
        jo.put("site", site);
        jo.put("level", level);
        jo.put("desc", desc);
        return jo;
    }

    private JSONArray createKPI_HYQX_SURGE_PROPORTION() {
        JSONArray array = new JSONArray();
//        double upperBound = 2.8, lowerBound = 0.5;
        List<Float> list1 = Arrays.asList(
                2.2f, 2.1f, 2.1f, 2.9f, 2.8f, 3f,
                2.9f, 2.8f, 2.7f, 2.7f, 2.8f, 2.6f,
                2f, 2.9f, 2.6f, 2.7f, 3.4f, 3f,
                1f, 1.2f, 1.7f, 1f, 1.4f, 1.7f
        );
        List<Float> list2 = Arrays.asList(
                3.2f, 3.1f, 3.0f, 2.9f, 2.8f, 3f,
                2.8f, 2.7f, 2.7f, 2.6f, 2.8f, 2.6f,
                2.2f, 2.6f, 2.8f, 2.7f, 3f, 3f,
                1f, 1.2f, 1.6f, 1.3f, 1.8f, 1.7f
        );
        List<Float> list3 = Arrays.asList(
                3.0f, 3.0f, 3.3f, 2.8f, 2.8f, 3f,
                2.9f, 2.8f, 2.7f, 2.8f, 2.8f, 2.6f,
                2f, 2.9f, 2.2f, 2.8f, 3f, 3f,
                1f, 1.2f, 1.5f, 1.9f, 1.4f, 1.7f
        );
        List<Float> list4 = Arrays.asList(
                1.2f, 2.1f, 2.1f, 2.9f, 2.8f, 3f,
                2.9f, 2.8f, 2.7f, 2.7f, 2.8f, 2.6f,
                2f, 2.9f, 2.8f, 2.7f, 2f, 2f,
                1.8f, 1.2f, 1.5f, 1.9f, 1.6f, 1.7f
        );
        array.add(createChart("A", list1));
        array.add(createChart("B", list2));
        array.add(createChart("C", list3));
        array.add(createChart("D", list4));
        return  array;
    }

    private JSONArray createKPI_HYQX_SHOAL_EFFECT() {
        JSONArray array = new JSONArray();
//        double upperBound = 2.2, lowerBound = 0.2;
        List<Float> list1 = Arrays.asList(
                4.2f, 4.1f, 4.1f, 3.9f, 3.8f, 3.7f,
                2.9f, 3.8f, 3.7f, 3.7f, 3.8f, 3.6f,
                2.7f, 2.9f, 2.6f, 2.7f, 3.4f, 3f,
                1.2f, 1.2f, 1.7f, 1f, 1.4f, 1.7f
        );
        List<Float> list2 = Arrays.asList(
                3.2f, 3.1f, 3.0f, 2.9f, 2.9f, 3.3f,
                2.8f, 2.8f, 2.7f, 2.6f, 2.8f, 2.6f,
                2.2f, 2.7f, 2.8f, 2.9f, 3.1f, 3.2f,
                1f, 1.2f, 1.6f, 1.3f, 1.8f, 1.8f
        );
        List<Float> list3 = Arrays.asList(
                3.0f, 3.0f, 3.3f, 3.8f, 3.8f, 3f,
                2.9f, 2.8f, 2.7f, 2.8f, 3.8f, 2.6f,
                2f, 2.9f, 2.2f, 2.8f, 3.2f, 3f,
                1.9f, 2.2f, 2.5f, 1.9f, 2.4f, 1.7f
        );
        List<Float> list4 = Arrays.asList(
                2.2f, 2.1f, 2.1f, 2.9f, 2.8f, 3f,
                3.9f, 2.8f, 2.7f, 2.7f, 2.8f, 2.6f,
                2.3f, 2.9f, 2.8f, 2.7f, 2f, 2f,
                2.8f, 2.2f, 2.5f, 2.9f, 2.6f, 2.7f
        );
        array.add(createChart("A", list1));
        array.add(createChart("B", list2));
        array.add(createChart("C", list3));
        array.add(createChart("D", list4));
        return  array;
    }

    private JSONArray createKPI_HYQX_WAVE_STEEPNESS() {
        JSONArray array = new JSONArray();
//        List<Integer> list = Arrays.asList(0, 1, 2, 3);
        List<Float> list1 = Arrays.asList(
                3f, 3f, 3f, 3f, 3f, 3f,
                2f, 2f, 2f, 2f, 1f, 1f,
                1f, 1f, 1f, 1f, 3f, 3f,
                3f, 3f, 3f, 3f, 3f, 3f
        );
        List<Float> list2 = Arrays.asList(
                2f, 2f, 2f, 3f, 3f, 3f,
                2f, 2f, 1f, 1f, 1f, 1f,
                1f, 1f, 1f, 1f, 3f, 3f,
                3f, 3f, 3f, 2f, 2f, 3f
        );
        List<Float> list3 = Arrays.asList(
                1f, 2f, 2f, 3f, 3f, 3f,
                2f, 2f, 1f, 1f, 1f, 1f,
                1f, 2f, 2f, 2f, 2f, 2f,
                3f, 3f, 3f, 2f, 2f, 3f
        );
        List<Float> list4 = Arrays.asList(
                1f, 1f, 1f, 1f, 1f, 3f,
                2f, 2f, 1f, 1f, 1f, 1f,
                1f, 3f, 3f, 3f, 3f, 3f,
                3f, 3f, 3f, 2f, 2f, 3f
        );
        array.add(createChart("A", list1));
        array.add(createChart("B", list2));
        array.add(createChart("C", list3));
        array.add(createChart("D", list4));

        return  array;
    }


    private JSONArray createKPI_HYQX_WARNING_TYPE() {
        JSONArray array = new JSONArray();
        int typhoon = 2;
        int wind = 44;
        int rain = 6;
        int thunder = 34;
        int fog = 15;
        array.add(createWarningType(typhoon, "台风"));
        array.add(createWarningType(wind, "大风"));
        array.add(createWarningType(rain, "暴雨"));
        array.add(createWarningType(thunder, "雷电"));
        array.add(createWarningType(fog, "大雾"));
        return array;
    }

    private JSONObject createWarningType(int value, String type) {
        JSONObject jo = new JSONObject();
        jo.put("value", value);
        jo.put("type", type);
        return jo;
    }

    private JSONArray createKPI_HYQX_SERVICE_PUBLISH() {
        JSONArray array = new JSONArray();
        JSONObject jo = new JSONObject();
        jo.put("qixiangzhuanbao", 96);
        jo.put("tianqizhuanbao", 9);
        jo.put("diantaiguangbogao", 692);
        jo.put("jinhaiyubao", 235);
        jo.put("haimianyubao", 365);
        jo.put("tianqiyujing", 918254);
        array.add(jo);
        return array;
    }

    private JSONArray createKPI_HYQX_ABNORMAL_WAVE_MONTH() {
        JSONArray array = new JSONArray();
        array.add(createMonthWave(1, 276298, 0.0166f));
        array.add(createMonthWave(2, 226295, 0.0133f));
        array.add(createMonthWave(3, 275156, 0.0156f));
        array.add(createMonthWave(4, 262471, 0.0175f));
        array.add(createMonthWave(5, 287056, 0.0219f));
        array.add(createMonthWave(6, 272903, 0.0205f));
        array.add(createMonthWave(7, 262797, 0.0198f));
        array.add(createMonthWave(8, 281797, 0.0167f));
        array.add(createMonthWave(9, 257408, 0.0132f));
        array.add(createMonthWave(10,245213, 0.0159f));
        array.add(createMonthWave(11,263715, 0.0152f));
        array.add(createMonthWave(12,279692, 0.0179f));
        return array;
    }

    private JSONObject createMonthWave(int month, int waveCount, float frequency) {
        JSONObject jo = new JSONObject();
        jo.put("date", month);
        jo.put("wave", waveCount);
        jo.put("frequency", frequency);
        return jo;
    }

    private JSONArray createKPI_HYQX_COURSE_INFLUENCE() {
        JSONArray array = new JSONArray();
        JSONObject jo1 = new JSONObject();
        jo1.put("site", "A");
        jo1.put("level", 1);
        jo1.put("jianglun", "一般");
        jo1.put("hailun", "一般");
        array.add(jo1);

        JSONObject jo2 = new JSONObject();
        jo2.put("site", "B");
        jo2.put("level", 3);
        jo2.put("jianglun", "严重");
        jo2.put("hailun", "较严重");
        array.add(jo2);

        JSONObject jo3 = new JSONObject();
        jo3.put("site", "C");
        jo3.put("level", 2);
        jo3.put("jianglun", "较严重");
        jo3.put("hailun", "一般");
        array.add(jo3);

        JSONObject jo4 = new JSONObject();
        jo4.put("site", "D");
        jo4.put("level", 1);
        jo4.put("jianglun", "一般");
        jo4.put("hailun", "一般");
        array.add(jo4);

        return array;
    }

    private void fillData(String apiName, JSONArray value) {
        logger.info(String.format("began task：%s", apiName));
        OceanDataEntity oceanDataEntity = new OceanDataEntity();
        oceanDataEntity.setName(apiName);
        oceanDataEntity.setValue(value);
        oceanDataDAO.updateOceanDataByName(oceanDataEntity);
    }
}
