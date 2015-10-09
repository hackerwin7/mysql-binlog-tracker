package com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe;

import java.nio.charset.Charset;
import java.util.Map;

/**
 * Created by hp on 7/17/15.
 */
public class ParsePipeConf extends FilePipeConf {

    /**
     * constructor to throw the super()'s Exception
     * @throws Exception
     */
    public ParsePipeConf() throws Exception {

    }

    /*getter*/
    public int getParseBatchCount() {
        return parseBatchCount;
    }

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

    public Charset getMysqlCharset() {
        return charset;
    }

    public Map<String, String> getFilter() {
        return filterMap;
    }
}
