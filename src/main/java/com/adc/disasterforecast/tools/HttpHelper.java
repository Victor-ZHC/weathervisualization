package com.adc.disasterforecast.tools;

import okhttp3.Call;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

public class HttpHelper {
    // logger for HttpHelper
    private static final Logger logger = LoggerFactory.getLogger(HttpHelper.class);

    public static JSONObject getDataByURL(String url) {
        return getJsonDataByURL(url);
    }

    public static <T> T getJsonDataByURL(String url) {
        logger.info("download JSON data from " + url);

        OkHttpClient okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(600, TimeUnit.SECONDS)
                .readTimeout(600, TimeUnit.SECONDS)
                .build();

        Request request = new Request.Builder()
                .url(url)
                .build();
        Call call = okHttpClient.newCall(request);

        try {
            Response response = call.execute();
            return (T) JSONValue.parse(response.body().string());
        } catch (IOException e) {
            logger.error(e.toString());
            e.printStackTrace();
            return null;
        }
    }
}
