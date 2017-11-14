package com.adc.disasterforecast.entity;

import org.json.simple.JSONArray;

public class DataEntity {
    private String name;
    private JSONArray value;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setValue(JSONArray value) {
        this.value = value;
    }

    public JSONArray getValue() {
        return value;
    }
}
