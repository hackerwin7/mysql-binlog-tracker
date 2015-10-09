package com.github.hackerwin7.mysql.binlog.tracker.checkpoint.checkpointer;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.ICheckpoint;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver.ZkExecutor;

/**
 * Created by hp on 7/15/15.
 */
public class MysqlEventPositionUtil4Zk implements ICheckpoint {

    /*data*/
    private ZookeeperConf zkc = null;

    /*driver*/
    private ZkExecutor zk = null;

    /**
     * constructor
     * @param zkServer
     * @throws Exception
     */
    public MysqlEventPositionUtil4Zk(String zkServer) throws Exception {
        zkc = new ZookeeperConf(zkServer);
        init();
    }

    /**
     * init the driver
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        zk = new ZkExecutor(zkc);
        zk.connect();
    }

    /**
     * read the checkpoint
     * @param zkPath
     * @return checkpoint
     * @throws Exception
     */
    @Override
    public String read(String zkPath) throws Exception {
        return zk.get(zkPath);
    }

    /**
     * write the value into zookeeper
     * @param path
     * @param value
     * @throws Exception
     */
    @Override
    public void write(String path, String value) throws Exception {
        zk.set(path, value);
    }

    /**
     * close the driver
     * @throws Exception
     */
    public void close() throws Exception {
        zk.disconnect();
        zk.close();
    }
}
