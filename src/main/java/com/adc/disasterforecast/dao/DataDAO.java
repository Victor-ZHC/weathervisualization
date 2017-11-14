package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.DataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class DataDAO {
    // logger for ExampleDAO
    private static final Logger logger = LoggerFactory.getLogger(DataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public DataEntity findExampleByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        DataEntity dataEntity = mongoTemplate.findOne(query, DataEntity.class);
        return dataEntity;
    }

    /**
     * 更新对象
     * @param dataEntity
     */
    public void updateExample(DataEntity dataEntity) {
        if (findExampleByName(dataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(dataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(dataEntity.getName()));
            Update update = new Update().set("value", dataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, DataEntity.class);
        }
    }
}
