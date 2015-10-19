package com.github.hackerwin7.mysql.binlog.tracker.pipe;

import java.util.concurrent.BlockingQueue;

/**
 * pipe with monitor record, many metrics for monitor
 * Created by hp on 10/14/15.
 */
public abstract class PipeMo extends Pipe {

    /**
     * constructor with queue
     * @param _queue
     */
    public PipeMo(BlockingQueue _queue) {
        super(_queue);
    }



}
