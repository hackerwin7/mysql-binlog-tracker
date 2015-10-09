package com.github.hackerwin7.mysql.binlog.tracker.controller;

import com.github.hackerwin7.mysql.binlog.tracker.commons.TrackerInterface;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import org.apache.log4j.Logger;

/**
 * magpie topology
 * Created by hp on 4/29/15.
 */
public class TopologyFramework {
    private Logger                                  logger                      = Logger.getLogger(TopologyFramework.class);

    private TrackerInterface tracker                     = null;
    private boolean                                 running                     = true;

    public TopologyFramework(TrackerInterface _tracker) {
        tracker = _tracker;
    }

    public void start() throws Exception {
        long interval = CommonConstants.DELAY_INTERVAL_FRAMEWORK;
        while (running) {
            try {
                tracker.prepare("");//no jobId until prepare() load config
                while (true) {
                    tracker.run();
                }
            } catch (Throwable e) {
                logger.error(e.getMessage() + ", it will restart the framework after " + interval + " ms......", e);
                delay(interval);
            }
        }
    }

    private void delay(long ms) throws Exception {
        Thread.sleep(ms);
    }
}
