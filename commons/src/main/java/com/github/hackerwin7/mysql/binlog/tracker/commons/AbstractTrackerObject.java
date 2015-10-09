package com.github.hackerwin7.mysql.binlog.tracker.commons;

import org.apache.log4j.Logger;

/**
 * Created by hp on 4/13/15.
 * the root object of mysql-binlog-tracker
 */
public abstract class AbstractTrackerObject extends AbstractDriver {

    protected final long interval = 60 * 1000;// now - heartBeatStmp must < interval

    protected boolean running = false;//running info
    protected long heartBeatStmp = 0;//heart beat info
    protected long startStmp = 0;//start time
    protected long stopStmp = 0;//stop time

    private Logger logger = Logger.getLogger(AbstractTrackerObject.class);

    /**
     * start the job
     * @throws Exception
     */
    public void start() throws Exception {
        if(running) {
            throw new Exception("the tracker had been running.... don't repeated start.");
        }

        logger.info("job started......");
        running = true;
        heartBeatStmp = System.currentTimeMillis();
        startStmp = heartBeatStmp;
        status = RUNNING;
    }

    /**
     * stop the job
     * @throws Exception
     */
    public void stop() throws Exception {
        if(!running) {
            throw new Exception("the tracker had stopped, don't repeated stop.");
        }

        logger.info("job stopped......");
        running = false;
        heartBeatStmp = System.currentTimeMillis();
        stopStmp = heartBeatStmp;
        status = STOPPING;
    }

    /**
     * heart beat record
     * @throws Exception
     */
    public void heartbeat() throws Exception {
        heartBeatStmp = System.currentTimeMillis();
        logger.info("heart beat record :" + heartBeatStmp);
    }

    /**
     *
     * @throws Exception
     */
    public void invoke() throws Exception {
        logger.info("the status is : [ " + status + " ], invoking......");
    }
}
