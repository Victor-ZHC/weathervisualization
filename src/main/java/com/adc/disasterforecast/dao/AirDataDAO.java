package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.AirDataEntity;
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
public class AirDataDAO {
    // logger for RainfallDataDAO
    private static final Logger logger = LoggerFactory.getLogger(AirDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public AirDataEntity findAirDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        AirDataEntity airDataEntity = mongoTemplate.findOne(query, AirDataEntity.class);
        return airDataEntity;
    }


    /**
     * 更新对象
     * @param airDataEntity
     */
    public void updateAirDataByName(AirDataEntity airDataEntity) {
        if (findAirDataByName(airDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(airDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(airDataEntity.getName()));
            Update update = new Update().set("value", airDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, AirDataEntity.class);
        }
    }
}
