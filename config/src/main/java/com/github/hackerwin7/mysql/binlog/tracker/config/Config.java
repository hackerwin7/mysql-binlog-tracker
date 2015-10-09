package com.github.hackerwin7.mysql.binlog.tracker.config;

import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 7/16/15.
 */
public abstract class Config {
    protected Logger logger = null;

    //id
    protected String jobId = "mysql-tracker";
    //mysql conf
    protected String username = "canal";
    protected String password = "canal";
    protected String address = "127.0.0.1";
    protected int myPort = 3306;
    protected long slaveId = 15879;
    //kafka data conf
    protected String kafkaDataZk = null;
    protected String kafkaDataZkRoot = null;
    protected String kafkaDataAcks = "-1";
    protected String kafkaDataTopic = null;
    //phenix monitor
    protected String kafkaPhZk = null;
    protected String kafkaPhZkRoot = null;
    protected String kafkaPhAcks = "1";
    protected String kafkaPhTopic = null;
    //charset mysql tracker
    protected Charset charset = Charset.forName("UTF-8");
    //filter same to parser, filter database and table
    protected Map<String, String> filterMap = new HashMap<String, String>();
    //sense filter, filter sensitive filed in a table
    protected Map<String, String> senseFilterMap = new HashMap<String, String>();
    //position
    protected String logfile = null;
    protected long offset = -1;
    protected long batchId = 0;
    protected long inId = 0;
    protected String CLASS_PREFIX = "classpath:";
    //checkpoint zk
    protected String checkpointZk = null;
    protected String checkpointZkRoot = null;
    //tracker conf
    protected int batchsize = 10000;
    protected int queuesize = 50000;
    protected int sumBatch = 5 * batchsize;
    protected int timeInterval = 5;
    protected int reInterval = 3;
    protected String filterRegex = ".*\\..*";
    protected int minsec = 60;
    protected int heartsec = 1 * 60;//10
    protected int retrys = 10;//if we retry 100 connect or send failed too, we will reload the job //interval time
    protected double mbUnit = 1024.0 * 1024.0;
    protected int spacesize = 8;//8 MB
    protected int monitorsec = 60;//1 minute
    protected int fetchBatchCount = 1;
    protected int parseBatchCount = 1;
    protected int persistBatchCount = 10000;
    protected long persistMaxMillions = 5000;

    /**
     * constructor call load method
     * @throws Exception
     */
    public Config() throws Exception {
        load();
    }

    /**
     * load the configuration
     * @throws Exception
     */
    protected abstract void load() throws Exception;

    /*getter*/
    public int getQueuesize() {
        return queuesize;
    }
}
