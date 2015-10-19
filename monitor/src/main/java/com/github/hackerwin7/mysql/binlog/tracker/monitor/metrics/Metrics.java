package com.github.hackerwin7.mysql.binlog.tracker.monitor.metrics;

import java.util.List;

/**
 * Created by hp on 10/14/15.
 */
public abstract class Metrics {

    /*constants*/
    protected static final String HEADER_LINE = "========================================================> ";
    protected static final String DATA_LINE = "---------------------> ";

    protected String describe = null;

    /**
     * constructor for describe
     * @param desc
     * @throws Exception
     */
    public Metrics(String desc) throws Exception {
        describe =desc;
    }

    /**
     * metrics for logger info show
     * @return show string
     */
    public abstract String toString();

    /**
     * multiline for show string
     * @return list of string
     */
    public abstract List<String> toStrings() throws Exception;

    /**
     * get describe info
     * @return info
     */
    public String getDesc() {
        return describe;
    }
}
