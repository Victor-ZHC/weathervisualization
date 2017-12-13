package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.BackUpDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class BackUpDataDAO {
    // logger for RealTimeControlDAO
    private static final Logger logger = LoggerFactory.getLogger(BackUpDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public BackUpDataEntity findBackUpDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        BackUpDataEntity backUpDataEntity = mongoTemplate.findOne(query, BackUpDataEntity.class);
        return backUpDataEntity;
    }


    /**
     * 更新对象
     * @param backUpDataEntity
     */
    public void updateBackUpDataByName(BackUpDataEntity backUpDataEntity) {
        if (findBackUpDataByName(backUpDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(backUpDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(backUpDataEntity.getName()));
            Update update = new Update().set("value", backUpDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, BackUpDataEntity.class);
        }
    }
}
