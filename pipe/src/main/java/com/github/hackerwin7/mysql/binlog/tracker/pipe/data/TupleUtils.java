package com.github.hackerwin7.mysql.binlog.tracker.pipe.data;

import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;

/**
 * Created by hp on 9/22/15.
 */
public class TupleUtils {
    public static boolean isBlank(Tuple tuple) throws Exception {
        if(tuple == null) {
            return true;
        } else {
            if(tuple.isBlank()) {
                return true;
            } else {
                return false;
            }
        }
    }
}
