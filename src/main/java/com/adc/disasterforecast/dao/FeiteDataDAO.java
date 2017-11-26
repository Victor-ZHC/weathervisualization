package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.FeiteDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class FeiteDataDAO {
    // logger for ExampleDAO
    private static final Logger logger = LoggerFactory.getLogger(FeiteDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public FeiteDataEntity findFeiteDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        FeiteDataEntity feiteDataEntity = mongoTemplate.findOne(query, FeiteDataEntity.class);
        return feiteDataEntity;
    }

    /**
     * 根据name字段查询对象
     * @param name
     * @param alarmId
     * @return
     */
    public FeiteDataEntity findFeiteDataByNameAndAlarmId(String name, String alarmId) {
        Query query = new Query(Criteria.where("name").is(name).and("alarmId").is(alarmId));
        FeiteDataEntity feiteDataEntity = mongoTemplate.findOne(query, FeiteDataEntity.class);
        return feiteDataEntity;
    }

    /**
     * 更新对象
     * @param feiteDataEntity
     */
    public void updateFeiteDataByName(FeiteDataEntity feiteDataEntity) {
        if (findFeiteDataByName(feiteDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(feiteDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(feiteDataEntity.getName()));
            Update update = new Update().set("value", feiteDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, FeiteDataEntity.class);
        }
    }

    /**
     * 更新对象
     * @param feiteDataEntity
     */
    public void updateFeiteDataByNameAndAlarmId(FeiteDataEntity feiteDataEntity) {
        if (findFeiteDataByNameAndAlarmId(feiteDataEntity.getName(), feiteDataEntity.getAlarmId()) == null) {
            logger.info("---add---");
            mongoTemplate.save(feiteDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(feiteDataEntity.getName()).and("alarmId").is(feiteDataEntity.getAlarmId()));
            Update update = new Update().set("value", feiteDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, FeiteDataEntity.class);
        }
    }
}
