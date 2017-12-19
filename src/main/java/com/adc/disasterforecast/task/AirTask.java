package com.adc.disasterforecast.task;

import com.adc.disasterforecast.dao.AirDataDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

@Component
public class AirTask {
    // logger for AirTask
    private static final Logger logger = LoggerFactory.getLogger(AirTask.class);
    private static final String PUDONG_AIRPORT_ICAO = "ZSPD";
    private static final String HONGQIAO_AIRPORT_ICAO = "ZSSS";

    @Autowired
    private AirDataDAO airDataDAO;

    @PostConstruct
    @Scheduled(cron = "0 0 0/1 * * ?")
    public void fetch() {
        try {

        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
