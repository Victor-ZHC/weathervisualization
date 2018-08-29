package com.adc.disasterforecast.dao;

import com.adc.disasterforecast.entity.HealthDataEntity;
import com.adc.disasterforecast.entity.po.HistoryHealthData;
import com.adc.disasterforecast.entity.po.WeatherDay;
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

@Component
public class WeatherDayDAO {
    // logger for HealthDataDAO
    private static final Logger logger = LoggerFactory.getLogger(WeatherDayDAO.class);

    @Autowired
    private MongoTemplate mongoTemplate;

    public void upsertWeatherDay(WeatherDay weatherDay) {
        mongoTemplate.save(weatherDay);
    }

    public List<HistoryHealthData> findHistoryHealthData(String beginTime) {
        Query query = new Query(where("FORECAST_TIME").gt(beginTime));
        return mongoTemplate.find(query, HistoryHealthData.class);
    }
}
