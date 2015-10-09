package com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.config.Config;
import com.github.hackerwin7.mysql.binlog.tracker.utils.file.FileUtils;

import java.nio.charset.Charset;
import java.util.Properties;

/**
 * Created by hp on 7/17/15.
 */
public class FilePipeConf extends Config {

    /**
     * default constructor throw the super()'s Exception
     * @throws Exception
     */
    public FilePipeConf() throws Exception {

    }

    /**
     * load the config parameter
     * @throws Exception
     */
    @Override
    public void load() throws Exception {
        Properties pro = new Properties();
        pro.load(FileUtils.file2in(CommonConstants.TRACKER_CONF_FILE, CommonConstants.TRACKER_CONF_SHELL));
        jobId = pro.getProperty("job.name");
        charset = Charset.forName(pro.getProperty("job.charset"));
        address = pro.getProperty("mysql.address");
        myPort = Integer.valueOf(pro.getProperty("mysql.port"));
        username = pro.getProperty("mysql.usr");
        password = pro.getProperty("mysql.psd");
        slaveId = Long.valueOf(pro.getProperty("mysql.slaveId"));
        kafkaDataZk = pro.getProperty("kafka.data.zkserver");
        kafkaDataZkRoot = pro.getProperty("kafka.data.zkroot");
        kafkaDataAcks = pro.getProperty("kafka.acks");
        kafkaDataTopic = pro.getProperty("kafka.data.topic.tracker");
        checkpointZk = pro.getProperty("zookeeper.servers");
        checkpointZkRoot = pro.getProperty("zookeeper.servers.root");
    }

    /*common getter*/
    public String getJobId() {
        return jobId;
    }
}
