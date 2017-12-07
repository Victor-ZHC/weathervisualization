package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.HistoryAnalysisDataEntity;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Component;

/**
 * @Description 历史数据的数据库操作
 * @Author lilin
 * @Create 2017-12-07 21:44
 **/
@Component
public class HistoryAnalysisDataDAO {
    // logger for RealTimeControlDAO
    private static final org.slf4j.Logger logger = LoggerFactory.getLogger(HistoryAnalysisDataDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    /**
     * 根据name字段查询对象
     * @param name
     * @return
     */
    public HistoryAnalysisDataEntity findHistoryAnalysisDataByName(String name) {
        Query query = new Query(Criteria.where("name").is(name));
        HistoryAnalysisDataEntity historyAnalysisDataEntity = mongoTemplate.findOne(query, HistoryAnalysisDataEntity.class);
        return historyAnalysisDataEntity;
    }

    /**
     * 更新对象
     * @param historyAnalysisDataEntity
     */
    public void updateHistoryAnalysisDataByName(HistoryAnalysisDataEntity historyAnalysisDataEntity) {
        if (findHistoryAnalysisDataByName(historyAnalysisDataEntity.getName()) == null) {
            logger.info("---add---");
            mongoTemplate.save(historyAnalysisDataEntity);
        } else {
            logger.info("---update---");
            Query query = new Query(Criteria.where("name").is(historyAnalysisDataEntity.getName()));
            Update update = new Update().set("value", historyAnalysisDataEntity.getValue());
            //更新查询返回结果集的第一条
            mongoTemplate.updateFirst(query, update, HistoryAnalysisDataEntity.class);
        }
    }
}
