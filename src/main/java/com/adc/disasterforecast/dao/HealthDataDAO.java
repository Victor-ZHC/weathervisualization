package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.HealthDataEntity;
import com.adc.disasterforecast.entity.po.HistoryHealthData;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

import java.util.List;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

@Component
public class HealthDataDAO {
    // logger for HealthDataDAO
    private static final Logger logger = LoggerFactory.getLogger(HealthDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public HealthDataEntity findHealthDataByName(String name) {
        Query query = new Query(where("name").is(name));
        HealthDataEntity healthDataEntity = mongoTemplate.findOne(query, HealthDataEntity.class);
        return healthDataEntity;
    }


    /**
     * 更新对象
     * @param healthDataEntity
     */
    public void updateHealthDataByName(HealthDataEntity healthDataEntity) {
        if (findHealthDataByName(healthDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(healthDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(where("name").is(healthDataEntity.getName()));
            Update update = new Update().set("value", healthDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, HealthDataEntity.class);
        }
    }

    public void upsertHistoryHealthData(HistoryHealthData data) {
        Query query = new Query(where("CROW").is(data.CROW)
                .and("FORECAST_TIME").is(data.FORECAST_TIME));
        Update update = new Update();
        update.set("WARNING_LEVEL", data.WARNING_LEVEL);
        update.set("WARNING_DESC", data.WARNING_DESC);
        update.set("WAT_GUIDE", data.WAT_GUIDE);

        mongoTemplate.upsert(query, update, HistoryHealthData.class);
    }

    public List<HistoryHealthData> findHistoryHealthData(String beginTime) {
        Query query = new Query(where("FORECAST_TIME").gt(beginTime));
        return mongoTemplate.find(query, HistoryHealthData.class);
    }
}
