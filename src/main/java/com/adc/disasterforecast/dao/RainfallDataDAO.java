package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.RainfallDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class RainfallDataDAO {
    // logger for RainfallDataDAO
    private static final Logger logger = LoggerFactory.getLogger(RainfallDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public RainfallDataEntity findRainfallDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        RainfallDataEntity rainfallDataEntity = mongoTemplate.findOne(query, RainfallDataEntity.class);
        return rainfallDataEntity;
    }


    /**
     * 更新对象
     * @param rainfallDataEntity
     */
    public void updateRainfallDataByName(RainfallDataEntity rainfallDataEntity) {
        if (findRainfallDataByName(rainfallDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(rainfallDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(rainfallDataEntity.getName()));
            Update update = new Update().set("value", rainfallDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, RainfallDataEntity.class);
        }
    }
}
