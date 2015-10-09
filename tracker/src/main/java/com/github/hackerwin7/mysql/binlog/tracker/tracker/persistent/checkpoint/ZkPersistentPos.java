package com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent.checkpoint;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver.ZkExecutor;
import org.apache.log4j.Logger;

/**
 * Created by hp on 4/28/15.
 */
public class ZkPersistentPos extends PositionPersistent implements Runnable {
    private Logger logger                      = Logger.getLogger(ZkPersistentPos.class);

    private ZkExecutor zkExecutor                  = null;
    private AbstractConf conf                        = null;

    public ZkPersistentPos(AbstractConf _conf) {
        conf = _conf;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _connect() throws Exception {
        zkExecutor = new ZkExecutor(new ZookeeperConf(conf.offsetZk));
        zkExecutor.connect();
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void disconnect() throws Exception {
        zkExecutor.disconnect();
        zkExecutor.close();
    }

    /**
     * schedule job
     */
    @Override
    public void run() {
        try {
            persistent();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     * persistent the position
     * @throws Exception
     */
    @Override
    public void persistent() throws Exception {
        if(position != null && position.isValid()) {
            String path = conf.offsetRoot + conf.offsetPersis + conf.jobId;
            String data = getPosition().getPersistent();
            if (zkExecutor.exists(path)) {
                zkExecutor.set(path, data);
            } else {
                zkExecutor.create(path, data);
            }
            logger.info("---------------------> persistent position : " + position.getPersistent());
        } else {
            throw new Exception("the position is null or invalid......");
        }
    }
}