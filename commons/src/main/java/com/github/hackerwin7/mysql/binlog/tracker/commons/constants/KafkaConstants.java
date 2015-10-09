package com.github.hackerwin7.mysql.binlog.tracker.commons.constants;

/**
 * Created by hp on 4/22/15.
 */
public class KafkaConstants {
    public static final String                      KAFKA_PROP_BROKERS                      = "metadata.broker.list";
    public static final String                      KAFKA_PROP_SERIALIZER                   = "serializer.class";
    public static final String                      KAFKA_PROP_KEY_SERIALIZER               = "key.serializer.class";
    public static final String                      KAFKA_PROP_PARTITIONER                  = "partitioner.class";
    public static final String                      KAFKA_PROP_ACKS                         = "request.required.acks";
    public static final String                      KAFKA_PROP_ZOOKEEPER_CONN               = "zookeeper.connect";
    public static final String                      KAFKA_PROP_GROUP_ID                     = "group.id";
    public static final String                      KAFKA_PROP_ZOOKEEPER_SESSION_TIMEOUT    = "zookeeper.session.timeout.ms";
    public static final String                      KAFKA_PROP_ZOOKEEPER_SYNC_TIME          = "zookeeper.sync.time.ms";
    public static final String                      KAFKA_PROP_AUTO_COMMIT_INTERVAL         = "auto.commit.interval.ms";
}
