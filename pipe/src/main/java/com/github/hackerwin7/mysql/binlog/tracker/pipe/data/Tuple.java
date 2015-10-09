package com.github.hackerwin7.mysql.binlog.tracker.pipe.data;

/**
 * meta data for pipe
 * Created by hp on 7/6/15.
 */
public abstract class Tuple {

    /*data*/
    protected String tId = null;


    /**
     * parse the tuple to bytes
     * @return byte array
     * @throws Exception
     */
    public abstract byte[] toBytes() throws Exception;

    /**
     * parse bytes to tuple
     * @param bytes byte array
     * @return tuple object
     * @throws Exception
     */
    public abstract Tuple parseFrom(byte[] bytes) throws Exception;

    /**
     * data is null or not
     * @return boolean
     * @throws Exception
     */
    public abstract boolean isBlank() throws Exception;

    /**
     * getter
     * @return tuple id
     */
    public String getTId() {
        return tId;
    }

    /**
     * persist tuple, can override or not
     * @return
     */
    public String toCheckpoint() {
        return null;
    }
}
