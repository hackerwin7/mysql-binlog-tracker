package com.github.hackerwin7.mysql.binlog.tracker.monitor.metrics;

import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.ArrayList;

/**
 * common monitor metrics for common use
 * Created by hp on 10/14/15.
 */
public class CommonMetrics extends Metrics {

    /*to string*/
    protected StringBuilder sb = new StringBuilder();

    /*fetch count*/
    public long fetchNum = 0;
    public long fetchBytes = 0;

    /*send count*/
    public long sendNum = 0;
    public long sendBytes = 0;

    /*op time*/
    public long sendMs = 0;
    public long fetchMs = 0;
    public long ts = 0;

    /*delay*/
    public long delayMs = 0;
    public long delayNum = 0;

    /*exception*/
    public long exNum = 0;
    public StringBuilder exStr = new StringBuilder();

    /*running status*/
    public String ip = null;
    public int reload = 0;//1 is reload once

    /**
     * desc constructor
     * @param desc
     * @throws Exception
     */
    public CommonMetrics(String desc) throws Exception {
        super(desc);
    }

    /**
     * metrics to string
     * @return metrics string
     */
    public String toString() {
        sb = new StringBuilder();
        sb.append(HEADER_LINE + describe + " monitor : \n");
        sb.append(DATA_LINE + "fetch num = " + fetchNum + "\n");
        sb.append(DATA_LINE + "fetch bytes = " + fetchBytes + "\n");
        sb.append(DATA_LINE + "send num = " + sendNum + "\n");
        sb.append(DATA_LINE + "send bytes = " + sendBytes + "\n");
        sb.append(DATA_LINE + "send during timestamp (one turn [single or batch], override to end) = " + sendMs + "\n");
        sb.append(DATA_LINE + "fetch during timestamp (one turn [single or batch], override to end) = " + fetchMs + "\n");
        sb.append(DATA_LINE + "timestamp = " + ts + "\n");
        sb.append(DATA_LINE + "delay ms (last message time) = " + delayMs + "\n");
        sb.append(DATA_LINE + "delay number (last message offset) = " + delayNum + "\n");
        sb.append(DATA_LINE + "exception num = " + exNum + "\n");
        sb.append(DATA_LINE + "exception message = " + exStr.toString() + "\n");
        sb.append(DATA_LINE + "ip = " + ip + "\n");
        sb.append(DATA_LINE + "is reload = " + reload + "\n");
        return sb.toString();
    }

    /**
     * multiline info for string show
     * @return list of string
     */
    public List<String> toStrings() throws Exception {
        List<String> list = new ArrayList<String>();
        String str = toString();
        String[] ss = StringUtils.split(str, "\n");
        for(String logInfo : ss) {
            if(!StringUtils.isBlank(logInfo)) {
                list.add(logInfo);
            }
        }
        return list;
    }
}
