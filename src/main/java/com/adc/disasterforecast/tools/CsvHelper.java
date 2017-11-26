package com.adc.disasterforecast.tools;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * ${DESCRIPTION}
 *
 * @author lilin
 * @create 2017-11-16 20:18
 **/
public class CsvHelper {

    private static final Logger logger = LoggerFactory.getLogger(CsvHelper.class);
    /**
     * @description:
     * @author: zhichengliu
     * @date: 17/11/26
     **/
    public static JSONArray getDataByURL(String url) {
        logger.info("download CSV data from " + url);

        OkHttpClient okHttpClient = new OkHttpClient();
        Request request = new Request.Builder()
                .url(url)
                .build();
        Call call = okHttpClient.newCall(request);

        JSONArray jsonArray = null;
        try {
            Response response = call.execute();
            jsonArray = getData(response.body().string());
        } catch (IOException e) {
            logger.error(e.toString());
            e.printStackTrace();
        }

        return jsonArray;
    }
    /**
     * @description:
     * @author: zhichengliu
     * @date: 17/11/26
     **/
    public static JSONArray getData(String data) {
        JSONArray csvData = new JSONArray();
        String [] rowName = null;
        for (String line: data.split("\n")) {
            line = line.replaceAll("\r", "");
            if (rowName == null) {
                rowName = line.split(",");
            }else {
                String [] row = line.split(",");
                JSONObject ele = new JSONObject();
                for (int i = 0; i < rowName.length; i ++) {
                    ele.put(rowName[i], row[i]);
                }
                csvData.add(ele);
            }

        }
        return csvData;
    }
}
