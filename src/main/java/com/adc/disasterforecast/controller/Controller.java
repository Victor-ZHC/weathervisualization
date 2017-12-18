package com.adc.disasterforecast.controller;

import com.adc.disasterforecast.dao.*;
import com.adc.disasterforecast.entity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import java.util.Date;

@RestController
public class Controller {
    // logger for controller
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    @Autowired
    FeiteDataDAO feiteDataDAO;

    @Autowired
    YPCaseDataDAO ypCaseDataDAO;

    @Autowired
    DisPreventDataDAO disPreventDataDAO;

    @Autowired
    RealTimeControlDAO realTimeControlDAO;

    @Autowired
    HistoryAnalysisDataDAO historyAnalysisDataDAO;

    @Autowired
    RainfallDataDAO rainfallDataDAO;

    @Autowired
    HealthDataDAO healthDataDAO;

    @Autowired
    OceanDataDAO oceanDataDAO;

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
     * handle all GET request from "fangzaijianzai" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/fangzaijianzai/{name}", method = RequestMethod.GET)
    @ResponseBody
    public DisPreventDataEntity getDisPreventDataByName(@PathVariable("name") String name) {
        logger.info("get request from /fangzaijianzai/" + name);
        DisPreventDataEntity disPreventDataEntity = disPreventDataDAO.findDisPreventDataByName(name);
        return disPreventDataEntity;
    }

    /**
     * handle all GET request from "realtimecontrol" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/shishiguankong/{name}", method = RequestMethod.GET)
    @ResponseBody
    public RealTimeControlDataEntity getRealTimeControlData(@PathVariable("name") String name) {
        logger.info("get request from /shishiguankong/" + name);
        RealTimeControlDataEntity realTimeControlDataEntity = realTimeControlDAO.findRealTimeControlDataByName(name);
        realTimeControlDataEntity.setTimestamp(new Date().getTime());
        return realTimeControlDataEntity;
    }

    /**
     * handle all GET request from "lishishuju" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/lishishuju/{name}", method = RequestMethod.GET)
    @ResponseBody
    public HistoryAnalysisDataEntity getHistoryAnalysisData(@PathVariable("name") String name) {
        logger.info("get request from /lishishuju/" + name);
        HistoryAnalysisDataEntity historyAnalysisDataEntity = historyAnalysisDataDAO.findHistoryAnalysisDataByName(name);
        return historyAnalysisDataEntity;
    }

    /**
     * handle all GET request from "baoyuneilao" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/baoyuneilao/{name}", method = RequestMethod.GET)
    @ResponseBody
    public RainfallDataEntity getRainfallData(@PathVariable("name") String name) {
        logger.info("get request from /baoyuneilao/" + name);
        RainfallDataEntity rainfallDataEntity = rainfallDataDAO.findRainfallDataByName(name);
        return rainfallDataEntity;
    }

    /**
     * handle all GET request from "jiankangqixiang" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/jiankangqixiang/{name}", method = RequestMethod.GET)
    @ResponseBody
    public HealthDataEntity getHealthData(@PathVariable("name") String name) {
        logger.info("get request from /jiankangqixiang/" + name);
        HealthDataEntity healthDataEntity = healthDataDAO.findHealthDataByName(name);
        return healthDataEntity;
    }

    /**
     * handle all GET request from "haiyangqixiang" page by name
     * @param name data name
     * @return dataEntity
     */
    @RequestMapping(value = "/haiyangqixiang/{name}", method = RequestMethod.GET)
    @ResponseBody
    public OceanDataEntity getOceanData(@PathVariable("name") String name) {
        logger.info("get request from /haiyangqixiang/" + name);
        OceanDataEntity oceanDataEntity = oceanDataDAO.findOceanDataByName(name);
        return oceanDataEntity;
    }
}
