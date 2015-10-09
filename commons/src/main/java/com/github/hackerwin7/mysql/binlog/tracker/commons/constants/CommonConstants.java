package com.github.hackerwin7.mysql.binlog.tracker.commons.constants;

/**
 * Created by hp on 4/20/15.
 */
public class CommonConstants {
    public static final double                      MB_UNIT                     = 1024.0 * 1024.0;
    public static final int                         MINUTE_UNIT                 = 60;
    public static final String                      TRAIN_URL                   = "http://train.bdp.jd.com/api/ztc/job/getJobConfig.ajax?jobId=";
    public static final String                      TRACKER_CONF_FILE           = "tracker.properties";
    public static final String                      TRACKER_CONF_SIMPLE_FILE    = "tracker-simple.properties";
    public static final String                      TRACKER_CONF_SHELL          = "tracker.conf";
    public static final String                      CLASSPATH_COLON             = "classpath:";
    public static final int                         RETRY_INTERVAL              = 1;
    public static final int                         RECONNECT_INTERVAL          = 5;
    public static final int                         RELOAD_INTERVAL             = 5;
    public static final String                      ZK_BROKERS_IDS              = "/brokers/ids";
    public static final int                         SYSTEM_CUR_INTERVAL         = 1000;
    public static final long                        THREAD_AWAT_TIME            = 30 * 1000;//30 seconds
    public static final int                         THREAD_COUNT_LIMIT          = 25;
    public static final int                         THREAD_SPLEEP_UNIT          = 200;
    public static final int                         THREAD_POOL_SIZE            = 5;
    public static final String                      LOG4J                       = "log4j.properties";
    public static final String                      LOG4J_SHELL                 = "tracker.log4j";
    public static final long                        DELAY_INTERVAL_FRAMEWORK    = 5000;
}
