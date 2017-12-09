package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.DisPreventDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

/**
 * @author zhichengliu
 * @create -12-06-21:07
 **/
@Component
public class DisPreventDataDAO {
    // logger for ExampleDAO
    private static final Logger logger = LoggerFactory.getLogger(DisPreventDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public DisPreventDataEntity findDisPreventDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        DisPreventDataEntity disPreventDataEntity = mongoTemplate.findOne(query, DisPreventDataEntity.class);
        return disPreventDataEntity;
    }

    /**
     * 更新对象
     * @param disPreventDataEntity
     */
    public void updateDisPreventDataByName(DisPreventDataEntity disPreventDataEntity) {
        if (findDisPreventDataByName(disPreventDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(disPreventDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(disPreventDataEntity.getName()));
            Update update = new Update().set("value", disPreventDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, DisPreventDataEntity.class);
        }
    }
}
