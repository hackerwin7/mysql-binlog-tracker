package com.github.hackerwin7.mysql.binlog.tracker.utils.system;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;

/**
 * Created by hp on 4/23/15.
 */
public class SystemUtils {
    public static int                               invoke_count                = 0;
    public static long                              time_stamp                  = 0;

    public long getCurrent() throws Exception {
        if(invoke_count == 0) {
            time_stamp = System.currentTimeMillis();
        }
        invoke_count ++;
        if(invoke_count == CommonConstants.SYSTEM_CUR_INTERVAL) {
            invoke_count = 0;
        }
        return time_stamp;
    }
}
