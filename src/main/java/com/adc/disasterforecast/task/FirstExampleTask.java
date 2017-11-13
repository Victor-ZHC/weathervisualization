package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

@Component
public class FirstExampleTask {
    // logger for FirstExampleTask
    private final static Logger logger = LoggerFactory.getLogger(FirstExampleTask.class);

    // dao Autowired
    @Autowired
    DataDAO dataDAO;

    private final static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private int counter = 1;
    private List<JSONObject> list = new ArrayList<>();

    @Scheduled(cron = "*/5 * * * * *")
    public void firstExp() throws InterruptedException {
        logger.info(String.format("---FirstTask执行第%d次---，现在时间是：%s", counter++, dateFormat.format(new Date())));

        JSONObject obj = new JSONObject();
        obj.put("first_name", "Victor");
        obj.put("last_name", "Zhou");
        obj.put("counter", counter);
        list.add(obj);

        JSONArray array = new JSONArray();
        array.addAll(list);

        DataEntity dataEntity = new DataEntity();
        dataEntity.setName("firstExp");
        dataEntity.setValue(array.toJSONString());

        dataDAO.updateExample(dataEntity);
    }
}
