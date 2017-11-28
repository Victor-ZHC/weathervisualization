package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.YPCaseDataEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

@Component
public class YPCaseDataDAO {
    // logger for ExampleDAO
    private static final Logger logger = LoggerFactory.getLogger(YPCaseDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public YPCaseDataEntity findYPCaseDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        YPCaseDataEntity ypCaseDataEntity = mongoTemplate.findOne(query, YPCaseDataEntity.class);
        return ypCaseDataEntity;
    }

    /**
     * 根据name字段查询对象
     * @param name
     * @param alarmId
     * @return
     */
    public YPCaseDataEntity findYPCaseDataByNameAndAlarmId(String name, String alarmId) {
        Query query = new Query(Criteria.where("name").is(name).and("alarmId").is(alarmId));
        YPCaseDataEntity ypCaseDataEntity = mongoTemplate.findOne(query, YPCaseDataEntity.class);
        return ypCaseDataEntity;
    }

    /**
     * 更新对象
     * @param ypCaseDataEntity
     */
    public void updateYPCaseDataByName(YPCaseDataEntity ypCaseDataEntity) {
        if (findYPCaseDataByName(ypCaseDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(ypCaseDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(ypCaseDataEntity.getName()));
            Update update = new Update().set("value", ypCaseDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, YPCaseDataEntity.class);
        }
    }

    /**
     * 更新对象
     * @param ypCaseDataEntity
     */
    public void updateYPCaseDataByNameAndAlarmId(YPCaseDataEntity ypCaseDataEntity) {
        if (findYPCaseDataByNameAndAlarmId(ypCaseDataEntity.getName(), ypCaseDataEntity.getAlarmId()) == null) {
            logger.info("---add---");
            mongoTemplate.save(ypCaseDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(ypCaseDataEntity.getName()).and("alarmId").is(ypCaseDataEntity
                    .getAlarmId()));
            Update update = new Update().set("value", ypCaseDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, YPCaseDataEntity.class);
        }
    }
}
