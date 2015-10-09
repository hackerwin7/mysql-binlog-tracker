package com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by hp on 7/16/15.
 */
public class FetchPipeConf extends FilePipeConf {

    /**
     * constructor throw the super exception
     * @throws Exception
     */
    public FetchPipeConf() throws Exception {

    }

    /*getter*/

    public String getMysqlUsr() {
        return username;
    }

    public String getMysqlPsd() {
        return password;
    }

    public String getMysqlAddr() {
        return address;
    }

    public int getMysqlPort() {
        return myPort;
    }

    public long getMysqlSlaveId() {
        return slaveId;
    }

    public Charset getMysqlCharset() {
        return charset;
    }

    public Map<String, String> getFilter() {
        return filterMap;
    }

    public int getFetchBatchCount() {
        return fetchBatchCount;
    }
}
