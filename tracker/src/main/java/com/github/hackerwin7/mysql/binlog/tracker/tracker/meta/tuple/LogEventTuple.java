package com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple;

import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;

/**
 * Created by hp on 7/16/15.
 */
public class LogEventTuple extends Tuple {

    /*data*/
    private LogEvent event = null;

    /**
     * constructor
     * @param _event
     * @throws Exception
     */
    public LogEventTuple(LogEvent _event) throws Exception {
        event = _event;
    }

    /**
     * parse tuple to bytes
     * @return bytes
     * @throws Exception
     */
    @Deprecated
    @Override
    public byte[] toBytes() throws Exception {
        /*no op*/
        return null;
    }

    /**
     * parse bytes to tuple
     * @param bytes byte array
     * @return
     * @throws Exception
     */
    @Deprecated
    @Override
    public Tuple parseFrom(byte[] bytes) throws Exception {
        /*no op*/
        return null;
    }

    /**
     * event is null or not
     * @return boolean
     * @throws Exception
     */
    @Override
    public boolean isBlank() throws Exception {
        if(event == null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * getter
     * @return LogEvent
     */
    public LogEvent getEvent() {
        return event;
    }
}
