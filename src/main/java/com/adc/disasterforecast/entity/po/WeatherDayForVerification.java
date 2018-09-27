package com.adc.disasterforecast.entity.po;

import java.util.List;

public class WeatherDayForVerification {
    private int year;
    private int month;
    private int day;
    private List<StationValue> stationValue;

    public int getYear() {
        return year;
    }

    public void setYear(int year) {
        this.year = year;
    }

    public int getMonth() {
        return month;
    }

    public void setMonth(int month) {
        this.month = month;
    }

    public int getDay() {
        return day;
    }

    public void setDay(int day) {
        this.day = day;
    }

    public List<StationValue> getStationValue() {
        return stationValue;
    }

    public void setStationValue(List<StationValue> stationValue) {
        this.stationValue = stationValue;
    }

    public class StationValue {
        public StationValue() {

        }

        public StationValue(String stationName, float value) {
            this.stationName = stationName;
            this.value = value;
        }

        private String stationName;
        private float value;

        public String getStationName() {
            return stationName;
        }

        public void setStationName(String stationName) {
            this.stationName = stationName;
        }

        public float getValue() {
            return value;
        }

        public void setValue(float value) {
            this.value = value;
        }

    }
}
