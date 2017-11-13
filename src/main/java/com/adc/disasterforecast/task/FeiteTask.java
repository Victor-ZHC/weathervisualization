package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.text.SimpleDateFormat;
import java.util.Date;

@Component
public class FeiteTask {
    // logger for SecondExampleTask
    private final static Logger logger = LoggerFactory.getLogger(FeiteTask.class);

    // dao Autowired
    @Autowired
    private DataDAO dataDAO;

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    private int counter = 1;

    @Scheduled(cron = "*/30 * * * * *")
    public void secondExp() throws InterruptedException {
        logger.info(String.format("---SecondTask执行第%d次---，现在时间是：%s", counter++, dateFormat.format(new Date())));

        JSONObject obj = new JSONObject();
        obj.put("first_name", "Victor");
        obj.put("last_name", "Zhou");
        obj.put("counter", counter);

        DataEntity dataEntity = new DataEntity();
        dataEntity.setName("secondExp");
        dataEntity.setValue(obj.toJSONString());

        dataDAO.updateExample(dataEntity);
    }
}
