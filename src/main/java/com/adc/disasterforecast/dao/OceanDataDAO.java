package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.OceanDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class OceanDataDAO {
    // logger for OceanDataDAO
    private static final Logger logger = LoggerFactory.getLogger(OceanDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public OceanDataEntity findOceanDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        OceanDataEntity oceanDataEntity = mongoTemplate.findOne(query, OceanDataEntity.class);
        return oceanDataEntity;
    }


    /**
     * 更新对象
     * @param oceanDataEntity
     */
    public void updateOceanDataByName(OceanDataEntity oceanDataEntity) {
        if (findOceanDataByName(oceanDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(oceanDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(oceanDataEntity.getName()));
            Update update = new Update().set("value", oceanDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, OceanDataEntity.class);
        }
    }
}
