package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.metadata;

/**
 * Created by hp on 4/22/15.
 */
public class KafkaMessage {
    public byte[]                                   key                         = null;
    public byte[]                                   msg                         = null;
    public long                                     offset                      = 0;
    public long                                     pullTime                    = 0;//warning: batch time not single time
    public int                                      partition                   = 0;
}
