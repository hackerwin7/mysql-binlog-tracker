package com.github.hackerwin7.mysql.binlog.tracker.manager;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by hp on 4/28/15.
 */
public class DriverPoolManager extends AbstractDriver implements Runnable {
    private Logger                                  logger                      = Logger.getLogger(DriverPoolManager.class);

    private List<AbstractDriver>                    pool                        = null;

    private int                                     scanStat                    = RELOAD;
    private int                                     retStat                     = RUNNING;

    /**
     * constructor
     */
    public DriverPoolManager() {
        pool = new ArrayList<AbstractDriver>();
    }

    /**
     *
     * @param driver
     */
    public void register(AbstractDriver driver) {
        if(driver != null) {
            pool.add(driver);
        }
    }

    /**
     *
     * @param index
     * @return
     */
    public AbstractDriver get(int index) {
        return pool.get(index);
    }

    /**
     * clear the collections
     */
    public void clear() {
        pool.clear();
    }

    /**
     *
     * @param stat
     * @return
     */
    public boolean scan(int stat) {
        logger.info("----------------------> scan the status: ");
        for(AbstractDriver driver : pool) {
            logger.info("=====> status :" + driver.getStat() + ", " + driver.toString());
            if(driver.getStat() == stat) {
                return true;
            }
        }
        return false;
    }

    /**
     * run the schedule
     */
    @Override
    public void run() {
        try {
            if(scan(scanStat)) {
                retStat = RELOAD;
            } else {
                retStat = RUNNING;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     *
     * @param scanStat
     */
    public void setScanStat(int scanStat) {
        this.scanStat = scanStat;
    }

    /**
     *
     * @return
     */
    public int getRetStat() {
        return retStat;
    }

}
