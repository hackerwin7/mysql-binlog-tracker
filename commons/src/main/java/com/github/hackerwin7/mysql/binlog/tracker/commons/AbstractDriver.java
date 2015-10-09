package com.github.hackerwin7.mysql.binlog.tracker.commons;

import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.commons.exception.ReloadException;
import org.apache.log4j.Logger;

/**
 * Created by hp on 4/20/15.
 */
public abstract class AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(AbstractDriver.class);

    //signal operation
    public static final int                         STOPPING                    = -1;
    public static final int                         RUNNING                     = 0;
    public static final int                         RETRY                       = 1;
    public static final int                         RECONNECT                   = 2;
    public static final int                         RELOAD                      = 3;
    public static final int                         RESTART                     = 4;

    //signal operation count
    public static final int                         RETRY_CNT                   = 3;
    public static final int                         RECONNECT_CNT               = 3;
    public static final int                         RELOAD_CNT                  = 9;
    public static final int                         TOTAL_RETRY_CNT             = 30;
    public static final int                         TOTAL_RECONN_CNT            = 10;

    //current status
    public int                                      status                      = STOPPING;

    //retry counter
    public int                                      retrys                      = 0;
    public int                                      reconnects                  = 0;
    public static int                               reloads                     = 0;

    /**
     * total retry reconnect reload counter
     * maybe reconnect is success but retry is always failed, so the reconnects always not ++
     * so , we set that if retry reload reconnect is totally > 100 (or 200) we reload it directly
     */

    /*total counter*/
    public int                                      totalRetrys                 = 0;
    public int                                      totalReconns                = 0;


    /**
     * catch exception and opStat() connect
     * such as:
     * try {
     *     _connect()
     * } catch (Exception e) {
     *     status = RECONNECT
     *     opStat()
     * }
     * @throws Exception
     */
    public void connect() throws Exception {
        if(status == RUNNING) {
            logger.info(this.toString() + " : the driver had been running, do not connect repeated!");
        } else {
            logger.info(this.toString() + " connecting......");
            status = RUNNING;
        }
        try {
            _connect();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            status = RECONNECT;
            opStat();
        }
    }

    /**
     * no catch exception, it is original connect, so the sub class will mainly override this method
     * @throws Exception
     */
    protected void _connect() throws Exception {

    }

    /**
     *
     * @throws Exception
     */
    public void disconnect() throws Exception {
        if(status == STOPPING) {
            logger.info("the driver had been stopping, do not disconnect repeated!");
        } else {
            logger.info("stopping......");
            status = STOPPING;
        }
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public boolean isConnected() throws Exception {
        return status == RUNNING;
    }

    /**
     *
     * @throws Exception
     */
    public void send() throws Exception {
        try {
            _send();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            status = RETRY;
            opStat();
        }
    }

    /**
     * original send
     * @throws Exception
     */
    protected void _send() throws Exception {

    }

    /**
     *
     * @throws Exception
     */
    public void pull() throws Exception {
        try {
            _pull();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     *
     * @throws Exception
     */
    public void _pull() throws Exception {

    }

    /**
     *
     * @throws Exception
     */
    public void run() throws Exception {
        logger.info("running......");
    }

    /**
     *
     * @throws Exception
     */
    public void close() throws Exception {
        logger.info("closing......");
    }

    /**
     *
     * @return
     */
    public int getStat() {
        return status;
    }

    /**
     *
     * @throws Exception
     */
    public void _retry() throws Exception {
        logger.info("retrying......");
        retrys = 0;
        while (retrys < RETRY_CNT) {
            try {
                totalRetrys++;
                _send();
                break;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                retrys++;
                delay(CommonConstants.RETRY_INTERVAL);
            }
        }
        if(totalRetrys > TOTAL_RETRY_CNT) {
            status = RELOAD;
            return;
        }
        if(retrys < RETRY_CNT) {
            status = RUNNING;
        } else {
            status = RECONNECT;
        }
    }

    /**
     *
     * @throws Exception
     */
    public void _reconnect() throws Exception {
        logger.info("reconnectiong......");
        disconnect();
        reconnects = 0;
        while (reconnects < RECONNECT_CNT) {
            try {
                totalReconns++;
                _connect();
                break;
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
                reconnects++;
                delay(CommonConstants.RECONNECT_INTERVAL);
                disconnect();
            }
        }
        if(totalReconns > TOTAL_RECONN_CNT) {
            status = RELOAD;
            return;
        }
        if(reconnects < RECONNECT_CNT) {
            status = RETRY;
        } else {
            status = RELOAD;
        }
    }

    /**
     * reload is a external command for magpie executor
     * @throws Exception
     */
    public void _reload() throws Exception {
        logger.info("reloading......");
        reloads++;
        logger.info("!!!!!reload count : " + reloads);
        if(reloads < RELOAD_CNT) {
            status = RELOAD;
            throw new ReloadException("driver send the reload exception......");
        } else {
            status = RESTART;
        }
    }

    /**
     *
     * @throws Exception
     */
    public void _restart() throws Exception {
        logger.info("restarting......");
        status = RESTART;
        logger.info("###############################  system exit ......");
        System.exit(1);//kill process and magpie will restart it
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public int opStat() throws Exception {
        while (status != RUNNING) {
            switch (status) {
                case RUNNING:
                    //no op
                    break;
                case STOPPING:
                    //no op
                    break;
                case RETRY:
                    _retry();
                    break;
                case RECONNECT:
                    _reconnect();
                    break;
                case RELOAD:
                    _reload();
                    break;
                case RESTART:
                    _restart();
                    break;
                default:
                    //no op
                    break;
            }
        }
        return status;
    }

    /**
     *
     * @param sec
     * @throws Exception
     */
    public void delay(int sec) throws Exception {
        Thread.sleep(sec * 1000);
    }
}
