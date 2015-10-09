package com.github.hackerwin7.mysql.binlog.tracker.manager;

import org.apache.log4j.Logger;

import java.nio.channels.Pipe;
import java.util.HashMap;
import java.util.Map;

/**
 * manage multiple pipes
 * Created by hp on 7/24/15.
 */
public class PipeManager {
    private Logger logger = Logger.getLogger(PipeManager.class);

    /*pipe pool*/
    private Map<String, Pipe> pool = new HashMap<String, Pipe>();

    /**
     * register pipe into manager pool
     * @param pipe, pipe
     * @throws Exception
     */
    public void register(String name, Pipe pipe) throws Exception {
        pool.put(name, pipe);
    }

    public void remove(String name) throws Exception {
        pool.remove(name);
    }

    /**
     * clear the pool
     * @throws Exception
     */
    public void clear() throws Exception {
        pool.clear();
    }
}
