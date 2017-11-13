package com.adc.disasterforecast.controller;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class FeiteController {
    // logger for ExampleController
    private final static Logger logger = LoggerFactory.getLogger(FeiteController.class);

    @Autowired
    DataDAO dataDAO;

    @RequestMapping("/getFirstExample")
    public DataEntity getFirstExample() {
        logger.info("---get request from /getFirstExample---");
        DataEntity dataEntity = dataDAO.findExampleByName("firstExp");
        return dataEntity;
    }

    @RequestMapping("/getSecondExample")
    public DataEntity getSecondExample() {
        logger.info("---get request from /getFirstExample---");
        DataEntity dataEntity = dataDAO.findExampleByName("secondExp");
        return dataEntity;
    }
}
