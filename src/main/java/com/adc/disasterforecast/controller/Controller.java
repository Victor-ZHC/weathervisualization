package com.adc.disasterforecast.controller;

import com.adc.disasterforecast.dao.FeiteDataDAO;
import com.adc.disasterforecast.dao.RealTimeControlDAO;
import com.adc.disasterforecast.dao.YPCaseDataDAO;
import com.adc.disasterforecast.entity.FeiteDataEntity;
import com.adc.disasterforecast.entity.RealTimeControlDataEntity;
import com.adc.disasterforecast.entity.YPCaseDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class Controller {
    // logger for controller
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    @Autowired
    FeiteDataDAO feiteDataDAO;

    @Autowired
    YPCaseDataDAO ypCaseDataDAO;

    @Autowired
    RealTimeControlDAO realTimeControlDAO;

    /**
     * handle all GET request from "feite" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/feite/{name}", method = RequestMethod.GET)
    @ResponseBody
    public FeiteDataEntity getFeiteDataByName(@PathVariable("name") String name) {
        logger.info("get request from /feite/" + name);
        FeiteDataEntity feiteDataEntity = feiteDataDAO.findFeiteDataByName(name);
        return feiteDataEntity;
    }

    /**
     * handle all GET request from "feite" page by name and alarmId
     * @param name data name
     * @param alarmId data alarmId
     * @return dataEntity
     */
    @RequestMapping(value = "/feite/{name}/{alarmId}", method = RequestMethod.GET)
    @ResponseBody
    public FeiteDataEntity getFeiteDataByNameAndAlarmId(@PathVariable("name") String name, @PathVariable("alarmId") String alarmId) {
        logger.info("get request from /feite/" + name + "/" + alarmId);
        FeiteDataEntity feiteDataEntity = feiteDataDAO.findFeiteDataByNameAndAlarmId(name, alarmId);
        return feiteDataEntity;
    }

    /**
     * handle all GET request from "ypcase" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/ypcase/{name}", method = RequestMethod.GET)
    @ResponseBody
    public YPCaseDataEntity getYPCaseData(@PathVariable("name") String name) {
        logger.info("get request from /ypcase/" + name);
        YPCaseDataEntity ypCaseDataEntity = ypCaseDataDAO.findYPCaseDataByName(name);
        return ypCaseDataEntity;
    }

    /**
     * handle all GET request from "ypcase" page by name and alarmId
     * @param name data name
     * @param alarmId data alarmId
     * @return dataEntity
     */
    @RequestMapping(value = "/ypcase/{name}/{alarmId}", method = RequestMethod.GET)
    @ResponseBody
    public YPCaseDataEntity getYPCaseDataByNameAndAlarmId(@PathVariable("name") String name, @PathVariable("alarmId")
            String alarmId) {
        logger.info("get request from /ypcase/" + name + "/" + alarmId);
        YPCaseDataEntity ypCaseDataEntity = ypCaseDataDAO.findYPCaseDataByNameAndAlarmId(name, alarmId);
        return ypCaseDataEntity;
    }

    /**
     * handle all GET request from "realtimecontrol" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/realtimecontrol/{name}", method = RequestMethod.GET)
    @ResponseBody
    public RealTimeControlDataEntity getRealTimeControlData(@PathVariable("name") String name) {
        logger.info("get request from /realtimecontrol/" + name);
        RealTimeControlDataEntity realTimeControlDataEntity = realTimeControlDAO.findRealTimeControlDataByName(name);
        return realTimeControlDataEntity;
    }
}
