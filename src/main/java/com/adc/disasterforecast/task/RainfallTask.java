//package com.adc.disasterforecast.task;
//
//import com.adc.disasterforecast.dao.RainfallDataDAO;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.scheduling.annotation.Scheduled;
//import org.springframework.stereotype.Component;
//
//import javax.annotation.PostConstruct;
//
//@Component
//public class RainfallTask {
//    // logger for RealTimeControlTask
//    private static final Logger logger = LoggerFactory.getLogger(RainfallTask.class);
//
//    // dao Autowired
//    @Autowired
//    private RainfallDataDAO rainfallDataDAO;
//
//    @PostConstruct
//    @Scheduled(cron = "0 */10 * * * *")
//    public void countSEEPER_SITE_TOP10() {
//
//    }
//}
