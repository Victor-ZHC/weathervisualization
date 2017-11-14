package com.adc.disasterforecast.controller;

import com.adc.disasterforecast.dao.DataDAO;
import com.adc.disasterforecast.entity.DataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

@RestController
public class Controller {
    // logger for controller
    private static final Logger logger = LoggerFactory.getLogger(Controller.class);

    @Autowired
    DataDAO dataDAO;

    /**
     * handle all GET request from "feite" page
     * @param name data name
     * @return
     */
    @RequestMapping(value = "/feite/{name}", method = RequestMethod.GET)
    @ResponseBody
    public DataEntity getFeiteData(@PathVariable("name") String name) {
        logger.info("get request from /feite/" + name);
        DataEntity dataEntity = dataDAO.findExampleByName(name);
        return dataEntity;
    }
}
