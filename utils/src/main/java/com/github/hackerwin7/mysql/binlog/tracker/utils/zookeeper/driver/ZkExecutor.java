package com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.utils.convert.ConvertUtils;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;

import java.util.List;

/**
 * Created by hp on 4/21/15.
 */
public class ZkExecutor extends AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(ZkExecutor.class);

    private ZooKeeper                               zooKeeper                   = null;
    private ZookeeperConf zkConf                      = null;

    private boolean                                 retBoolean                  = false;
    private String                                  retString                   = "";
    private List<String>                            retListString               = null;

    public static final int                         SIG_GET                     = 0;
    public static final int                         SIG_SET                     = 1;
    public static final int                         SIG_EXISTS                  = 2;
    public static final int                         SIG_CREATE                  = 3;
    public static final int                         SIG_CHILDREN                = 4;
    public static final int                         SIG_DELETE                  = 5;

    private int                                     signal                      = 0;
    private String                                  zkPath                      = "";
    private String                                  zkData                      = "";

    /**
     *
     * @param zc
     */
    public ZkExecutor(ZookeeperConf zc) {
        zkConf = new ZookeeperConf(zc);
    }

    /**
     *
     * @return
     */
    public ZookeeperConf getZkConf() {
        return zkConf;
    }

    /**
     *
     * @param zkConf
     */
    public void setZkConf(ZookeeperConf zkConf) {
        this.zkConf = zkConf;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _connect() throws Exception {
        if(zkConf == null) {
            throw new Exception("zookeeper config is null, can not instance zookeeper connection!");
        }
        zooKeeper = new ZooKeeper(zkConf.getConnStr(), zkConf.timeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                logger.info("zookeeper watcher:" + event.getType());
            }
        });
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void disconnect() throws Exception {
        super.disconnect();
        if(zooKeeper != null) {
            zooKeeper.close();
        }
    }

    /**
     *
     * @return
     * @throws Exception
     */
    @Override
    public boolean isConnected() throws Exception {
        return super.isConnected();
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _send() throws Exception {
        switch (signal) {
            case SIG_GET:
                retString = _get(zkPath);
                break;
            case SIG_CHILDREN:
                retListString = _getChildren(zkPath);
                break;
            case SIG_CREATE:
                _create(zkPath, zkData);
                break;
            case SIG_DELETE:
                _delete(zkPath);
                break;
            case SIG_EXISTS:
                retBoolean = _exists(zkPath);
                break;
            case SIG_SET:
                _set(zkPath, zkData);
                break;
            default:
                //no op
                break;
        }
    }

    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    private boolean _exists(String path) throws Exception {
        if(zooKeeper.exists(path, false) == null) {
            return false;
        } else {
            return true;
        }
    }

    /**
     *
     * @param path
     * @param data
     * @throws Exception
     */
    private void _create(String path, String data) throws Exception {
        if(!_exists(path)) {
            String[] ss = path.split("/");
            String sum = ss[0];
            if(!_exists(sum)) {
                zooKeeper.create(sum, ConvertUtils.String2Bytes(data), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            }
            for (int i = 1; i <= ss.length - 1; i++) {
                sum += "/";
                sum += ss[i];
                if(!_exists(sum)) {
                    zooKeeper.create(sum, ConvertUtils.String2Bytes(data), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                }
            }
        }
    }

    /**
     *
     * @param path
     * @param data
     * @throws Exception
     */
    private void _set(String path, String data) throws Exception {
        if (_exists(path)) {
            zooKeeper.setData(path, ConvertUtils.String2Bytes(data), -1);
        } else {
            _create(path, data);
        }
    }

    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    private String _get(String path) throws Exception {
        if(!_exists(path)) {
            return null;
        } else {
            byte[] bytes = zooKeeper.getData(path, new Watcher() {
                @Override
                public void process(WatchedEvent event) {
                    logger.info("get data watcher : " + event.getType());
                }
            }, null);
            return ConvertUtils.Bytes2String(bytes);
        }
    }

    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    private List<String> _getChildren(String path) throws Exception {
        if(!_exists(path)) {
            return null;
        } else {
            List<String> childList = zooKeeper.getChildren(path, false);
            return childList;
        }
    }

    /**
     *
     * @param path
     * @throws Exception
     */
    private void _delete(String path) throws Exception {
        if(_exists(path)) {
            zooKeeper.delete(path, -1);
        }
    }

    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    public boolean exists(String path) throws Exception {
        zkPath = path;
        signal = SIG_EXISTS;
        send();
        return retBoolean;
    }

    /**
     *
     * @param path
     * @param data
     * @throws Exception
     */
    public void create(String path, String data) throws Exception {
        zkPath = path;
        zkData = data;
        signal = SIG_CREATE;
        send();
    }

    /**
     *
     * @param path
     * @param data
     * @throws Exception
     */
    public void set(String path, String data) throws Exception {
        zkPath = path;
        zkData = data;
        signal = SIG_SET;
        send();
    }

    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    public String get(String path) throws Exception {
        zkPath = path;
        signal = SIG_GET;
        send();
        return retString;
    }

    /**
     *
     * @param path
     * @return
     * @throws Exception
     */
    public List<String> getChildren(String path) throws Exception {
        zkPath = path;
        signal = SIG_CHILDREN;
        send();
        return retListString;
    }

    /**
     *
     * @param path
     * @throws Exception
     */
    public void delete(String path) throws Exception {
        zkPath = path;
        signal = SIG_DELETE;
        send();
    }
}
