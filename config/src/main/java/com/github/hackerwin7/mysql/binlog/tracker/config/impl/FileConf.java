package com.github.hackerwin7.mysql.binlog.tracker.config.impl;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.utils.file.FileUtils;
import org.apache.log4j.Logger;

import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Created by hp on 4/20/15.
 */
public class FileConf extends AbstractConf {
    private Logger                                  logger                      = Logger.getLogger(FileConf.class);

    /**
     *
     * @param job_id
     */
    public FileConf(String job_id) {
        jobId = job_id;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void load() throws Exception {
        super.load();
        Properties pro = new Properties();
        pro.load(FileUtils.file2in(CommonConstants.TRACKER_CONF_FILE, CommonConstants.TRACKER_CONF_SHELL));
        jobId = pro.getProperty("job.name");
        mysqlCharset = Charset.forName(pro.getProperty("job.charset"));
        mysqlAddr = pro.getProperty("mysql.address");
        mysqlPort = Integer.valueOf(pro.getProperty("mysql.port"));
        mysqlUsr = pro.getProperty("mysql.usr");
        mysqlPsd = pro.getProperty("mysql.psd");
        mysqlSlaveId = Long.valueOf(pro.getProperty("mysql.slaveId"));
        kafkaZk = pro.getProperty("kafka.data.zkserver");
        kafkaRoot = pro.getProperty("kafka.data.zkroot");
        kafkaAcks = pro.getProperty("kafka.acks");
        kafkaTopic = pro.getProperty("kafka.data.topic.tracker");
        monitorZk = pro.getProperty("kafka.monitor.zkserver");
        monitorRoot = pro.getProperty("kafka.monitor.zkroot");
        monitorTopic = pro.getProperty("kafka.monitor.topic");
        offsetZk = pro.getProperty("zookeeper.servers");
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void statis() throws Exception {
        super.statis();
    }

    @Override
    public void clear() throws Exception {
        super.clear();
    }
}
