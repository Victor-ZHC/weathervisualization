package com.adc.disasterforecast.tools;

import org.json.simple.JSONObject;

import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Queue;

/**
 * PriorityQueueHelper
 *
 * @author zhichengliu
 * @create -11-27-14:49
 **/

public class PriorityQueueHelper {
    public static Queue<JSONObject> getPriorityQueue(int flag, int size, String key){
        return new PriorityQueue<>(size, new Comparator<JSONObject>() {
            @Override
            public int compare(JSONObject o1, JSONObject o2) {
                Double x = Double.parseDouble((String) o1.get(key));
                Double y = Double.parseDouble((String) o2.get(key));
                return x < y ? flag*-1 : (x == y ? 0: flag*1);
            }
        });
    }
}
