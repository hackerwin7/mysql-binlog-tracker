package com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import org.apache.log4j.Logger;

/**
 * Created by hp on 4/21/15.
 */
public class ZookeeperConf extends AbstractConf {
    private Logger                                  logger                      = Logger.getLogger(ZookeeperConf.class);

    public String                                   zkStr                       = "127.0.0.1";
    public int                                      port                        = 2181;
    public int                                      timeout                     = 100000;

    /**
     *
     * @param zc
     */
    public ZookeeperConf(ZookeeperConf zc) {
        zkStr = zc.zkStr;
        port = zc.port;
    }

    public ZookeeperConf(String zk_str, int _port) {
        zkStr = zk_str;
        port = _port;
    }

    public ZookeeperConf(String zkConn) throws Exception {
        int ind = zkConn.lastIndexOf(":");
        zkStr = zkConn.substring(0, ind);
        port = Integer.valueOf(zkConn.substring(ind + 1, zkConn.length() - 1));
    }

    /**
     *
     * @return
     */
    public String getConnStr() {
        return zkStr + ":" + port;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void statis() throws Exception {
        logger.info("zk conf:" + getConnStr());
    }
}
