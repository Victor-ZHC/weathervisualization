package com.adc.disasterforecast.entity.po;


import org.json.simple.JSONObject;

public class HistoryHealthData {
    public long ID;
    public String CROW;
    public String FORECAST_TIME;
    public long WARNING_LEVEL;
    public String WARNING_DESC;
    public String INFLU;
    public String WAT_GUIDE;

    public HistoryHealthData() {
    }

    public HistoryHealthData(JSONObject jo) {
        ID = (long) jo.get("ID");
        CROW = (String) jo.get("CROW");
        FORECAST_TIME = (String) jo.get("FORECAST_TIME");
        WARNING_LEVEL = (long) jo.get("WARNING_LEVEL");
        WARNING_DESC = (String) jo.get("WARNING_DESC");
        INFLU = (String) jo.get("INFLU");
        WAT_GUIDE = (String) jo.get("WAT_GUIDE");
    }
}
