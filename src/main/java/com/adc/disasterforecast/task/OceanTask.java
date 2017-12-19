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
import java.text.DecimalFormat;

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
//        fillData(OceanTaskName.HYQX_WAVE_STEEPNESS, createKPI_HYQX_WAVE_STEEPNESS());
//        fillData(OceanTaskName.HYQX_SHOAL_EFFECT, createKPI_HYQX_SHOAL_EFFECT());
//        fillData(OceanTaskName.HYQX_SURGE_PROPORTION, createKPI_HYQX_SURGE_PROPORTION());
            fillData(OceanTaskName.HYQX_NAVIGATION_BROADCAST, createKPI_HYQX_NAVIGATION_BROADCAST());
//        fillData(OceanTaskName.HYQX_SYNC_ROLLING, createKPI_HYQX_SYNC_ROLLING());
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

//    private JSONArray createKPI_HYQX_SYNC_ROLLING() {
//    }

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

//    private JSONArray createKPI_HYQX_SURGE_PROPORTION() {
//    }

//    private JSONArray createKPI_HYQX_SHOAL_EFFECT() {
//    }

//    private JSONArray createKPI_HYQX_WAVE_STEEPNESS() {
//    }

    private JSONArray createKPI_HYQX_WARNING_TYPE() {
        JSONArray array = new JSONArray();
        int typhoon = 2;
        int wind = 44;
        int rain = 6;
        int thunder = 34;
        int fog = 15;
        int total = typhoon + wind + rain + thunder + fog;
        array.add(createWarningType((float) typhoon / total * 100, "台风"));
        array.add(createWarningType((float) wind / total * 100, "大风"));
        array.add(createWarningType((float) rain / total * 100, "暴雨"));
        array.add(createWarningType((float) thunder / total * 100, "雷电"));
        array.add(createWarningType((float) fog / total * 100, "大雾"));
        return array;
    }

    private JSONObject createWarningType(float value, String type) {
        value = Float.valueOf(new DecimalFormat(".00").format(value));
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
        jo1.put("jianglun", "较严重");
        jo1.put("hailun", "一般");
        array.add(jo1);

        JSONObject jo2 = new JSONObject();
        jo2.put("site", "B");
        jo2.put("level", 3);
        jo2.put("jianglun", "较严重");
        jo2.put("hailun", "一般");
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
        jo4.put("jianglun", "较严重");
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
