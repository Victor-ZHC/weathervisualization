package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.RealTimeControlDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class RealTimeControlDAO {
    // logger for RealTimeControlDAO
    private static final Logger logger = LoggerFactory.getLogger(RealTimeControlDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public RealTimeControlDataEntity findRealTimeControlDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        RealTimeControlDataEntity realTimeControlDataEntity = mongoTemplate.findOne(query, RealTimeControlDataEntity.class);
        return realTimeControlDataEntity;
    }


    /**
     * 更新对象
     * @param realTimeControlDataEntity
     */
    public void updateRealTimeControlDataByName(RealTimeControlDataEntity realTimeControlDataEntity) {
        if (findRealTimeControlDataByName(realTimeControlDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(realTimeControlDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(realTimeControlDataEntity.getName()));
            Update update = new Update().set("value", realTimeControlDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, RealTimeControlDataEntity.class);
        }
    }
}
