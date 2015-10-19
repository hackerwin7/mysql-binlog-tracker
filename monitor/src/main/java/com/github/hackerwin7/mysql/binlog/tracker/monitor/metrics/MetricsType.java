package com.github.hackerwin7.mysql.binlog.tracker.monitor.metrics;

import java.util.HashMap;
import java.util.Map;

/**
 * id and type desc for monitor type
 * Created by hp on 10/19/15.
 */
public class MetricsType {
    public static Map<Integer, String> MoType = new HashMap<Integer, String>();

    static {
        MoType.put(1, "fetch");
        MoType.put(2, "")
    }
}
