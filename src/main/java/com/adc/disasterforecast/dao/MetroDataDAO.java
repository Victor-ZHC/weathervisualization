package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.AirDataEntity;
import com.adc.disasterforecast.entity.MetroDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class MetroDataDAO {
    // logger for RainfallDataDAO
    private static final Logger logger = LoggerFactory.getLogger(MetroDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public MetroDataEntity findMetroDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        MetroDataEntity metroDataEntity = mongoTemplate.findOne(query, MetroDataEntity.class);
        return metroDataEntity;
    }


    /**
     * 更新对象
     * @param metroDataEntity
     */
    public void updateMetroDataByName(MetroDataEntity metroDataEntity) {
        if (findMetroDataByName(metroDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(metroDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(metroDataEntity.getName()));
            Update update = new Update().set("value", metroDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, MetroDataEntity.class);
        }
    }
}
