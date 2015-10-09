package com.github.hackerwin7.mysql.binlog.tracker.commons;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by hp on 4/20/15.
 */
public abstract class AbstractConf {
    private Logger                                  logger                      = Logger.getLogger(AbstractConf.class);

    //mysql config
    public String                                   mysqlAddr                   = "127.0.0.1";
    public int                                      mysqlPort                   = 3306;
    public String                                   mysqlUsr                    = "mysql";
    public String                                   mysqlPsd                    = "mysql";
    public long                                     mysqlSlaveId                = 123407;
    public Charset                                  mysqlCharset                = Charset.forName("UTF-8");

    //kafka config
    public String                                   kafkaAcks                   = "-1";
    public String                                   kafkaZk                     = "172.22.178.127:2181,172.22.178.128:2181,172.22.178.179:2181";
    public String                                   kafkaRoot                   = "/kafka";
    public String                                   kafkaTopic                  = "tracker-log-mysql-172174322";

    //monitor config
    public String                                   monitorZk                   = "172.17.25.27:2181,172.17.25.28:2181,172.17.25.30:2181";
    public String                                   monitorRoot                 = "/kafka";
    public String                                   monitorTopic                = "magpie_eggs_topic";

    //tracker config
    public String                                   jobId                       = "0";

    //tracker position
    public String                                   logfile                     = "mysql-bin.000001";
    public long                                     offset                      = -1;
    public long                                     batchId                     = 0;
    public long                                     inId                        = 0;
    public long                                     mid                         = 0;

    //tracker offset zk
    public String                                   offsetZk                    = "172.22.178.86:2181,172.22.178.87:2181,172.22.178.88:2181,172.22.178.89:2181,172.22.178.90:2181";
    public String                                   offsetRoot                  = "/checkpoint";
    public final String                             offsetPersis                = "/persistence";
    public final String                             offsetMinute                = "/minutes";

    //tracker filter
    public Map<String, String> filterMap                                        = new HashMap<String, String>();

    //tracker sensitive
    public Map<String, String>                      senseMap                    = new HashMap<String, String>();

    //tracker threshold constants
    public int                                      batch_size                  = 500000;
    public int                                      queue_size                  = 1000000;
    public long                                     interval_async              = 200;
    public long                                     interval_sync               = 1000;
    public int                                      min_sec                     = 1 * CommonConstants.MINUTE_UNIT;
    public int                                      heart_sec                   = 1 * CommonConstants.MINUTE_UNIT;
    public int                                      monitor_sec                 = 1 * CommonConstants.MINUTE_UNIT;
    public double                                   msg_limit                   = 1 * CommonConstants.MB_UNIT;
    public double                                   msgs_limit                  = 8 * CommonConstants.MB_UNIT;
    public double                                   msgs_compress_limit         = 1 * CommonConstants.MB_UNIT;

    /*
     *
     * @throws Exception
     */
    public void load() throws Exception {
        logger.info("load config......");
    }

    /**
     *
     * @throws Exception
     */
    public void clear() throws Exception {
        logger.info("clear config......");
        if(filterMap != null) {
            filterMap.clear();
        }
        if(senseMap != null) {
            senseMap.clear();
        }
    }

    /**
     *
     * @throws Exception
     */
    public void statis() throws Exception {
        logger.info("load the configuration's content is ");
        logger.info("------------------------------------------config start-------------------------------------------");
        logger.info("job id:" + jobId);
        logger.info("mysql address:" + mysqlAddr);
        logger.info("mysql port:" + mysqlPort);
        logger.info("mysql user:" + mysqlUsr);
        logger.info("mysql password:" + mysqlPsd);
        logger.info("mysql slave id:" + mysqlSlaveId);
        logger.info("mysql charset:" + mysqlCharset);
        logger.info("kafka acknowledges:" + kafkaAcks);
        logger.info("kafka zk:" + kafkaZk);
        logger.info("kafka root:" + kafkaRoot);
        logger.info("kafka topic:" + kafkaTopic);
        logger.info("monitor zk:" + monitorZk);
        logger.info("monitor root:" + monitorRoot);
        logger.info("monitor topic:" + monitorTopic);
        logger.info("log file:" + logfile);
        logger.info("log offset:" + offset);
        logger.info("batch id:" + batchId);
        logger.info("inner batch id:" + inId);
        logger.info("offset zk:" + offsetZk);
        logger.info("offset root:" + offsetRoot);
        logger.info("offset persistence:" + offsetPersis);
        logger.info("offset minute:" + offsetMinute);
        logger.info("filter map:" + filterMap.toString());
        logger.info("sensitive map:" + senseMap.toString());
        logger.info("batch size:" + batch_size + " messages");
        logger.info("queue size:" + queue_size + " messages");
        logger.info("interval async send:" + interval_async + " ms");
        logger.info("interval sync send:" + interval_sync + " ms");
        logger.info("minute interval:" + min_sec + " s");
        logger.info("heartbeat interval:" + heart_sec + " s");
        logger.info("monitor send interval:" + monitor_sec + " s");
        logger.info("single message limit:" + msg_limit + " bytes");
        logger.info("single batch messages limit:" + msgs_limit + " bytes");
        logger.info("single compressed batch messages limit:" + msgs_compress_limit + " bytes");
        logger.info("------------------------------------------config end-------------------------------------------");
    }
}
