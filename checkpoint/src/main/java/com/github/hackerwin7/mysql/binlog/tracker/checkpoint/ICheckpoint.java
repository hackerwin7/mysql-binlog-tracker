package com.github.hackerwin7.mysql.binlog.tracker.checkpoint;

/**
 * interface for checkpoint util, checkpoint is a serialized string for position
 * Created by hp on 7/2/15.
 */
public interface ICheckpoint {

    /**
     * initialize the instance for checkpointer  driver (zk, hbase etc.)
     * @throws Exception
     */
    public void init() throws Exception;

    /**
     * read the checkpoint from service
     * @param key
     * @return checkpoint
     * @throws Exception
     */
    public String read(String key) throws Exception;

    /**
     * write the checkpoint into service
     * @param key
     * @param value
     * @throws Exception
     */
    public void write(String key, String value) throws Exception;
}
