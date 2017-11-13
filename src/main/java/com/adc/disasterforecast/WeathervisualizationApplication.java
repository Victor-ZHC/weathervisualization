package com.adc.disasterforecast;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@EnableScheduling
@SpringBootApplication
public class WeathervisualizationApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeathervisualizationApplication.class, args);
    }
}
