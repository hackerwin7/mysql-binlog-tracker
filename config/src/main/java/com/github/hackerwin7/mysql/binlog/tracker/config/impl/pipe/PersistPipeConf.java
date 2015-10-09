package com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe;

/**
 * Created by hp on 7/17/15.
 */
public class PersistPipeConf extends FilePipeConf {

    /**
     * constructor, throw the super()'s Exception
     * @throws Exception
     */
    public PersistPipeConf() throws Exception {

    }

    /*getter*/
    public int getPersistBatchCount() {
        return persistBatchCount;
    }

    public String getKafkaZk() {
        return kafkaDataZk;
    }

    public String getKafkaZkRoot() {
        return kafkaDataZkRoot;
    }

    public String getKafkaAcks() {
        return kafkaDataAcks;
    }

    public String getKafkaTopic() {
        return kafkaDataTopic;
    }

    public long getPersistMaxMills() {
        return persistMaxMillions;
    }
}
