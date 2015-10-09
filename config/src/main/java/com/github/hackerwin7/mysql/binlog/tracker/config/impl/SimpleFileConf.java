package com.github.hackerwin7.mysql.binlog.tracker.config.impl;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.file.FileUtils;
import org.apache.log4j.Logger;

import java.util.Properties;

/**
 * Created by hp on 4/20/15.
 */
public class SimpleFileConf extends AbstractConf {
    private Logger                                  logger                      = Logger.getLogger(SimpleFileConf.class);

    /**
     *
     * @param job_id
     */
    public SimpleFileConf(String job_id) {
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
        pro.load(FileUtils.file2in(CommonConstants.TRACKER_CONF_SIMPLE_FILE, CommonConstants.TRACKER_CONF_SHELL));
        mysqlAddr = pro.getProperty("address");
        mysqlPort = Integer.valueOf(pro.getProperty("port"));
        mysqlSlaveId = Long.valueOf(pro.getProperty("slaveId"));
        mysqlUsr = pro.getProperty("username");
        mysqlPsd = pro.getProperty("password");
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void statis() throws Exception {
        super.statis();
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void clear() throws Exception {
        super.clear();
    }
}
