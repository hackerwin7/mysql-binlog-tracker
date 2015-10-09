package com.github.hackerwin7.mysql.binlog.tracker.pipe.channel;

import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.TupleUtils;
import org.apache.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by hp on 9/22/15.
 */
public abstract class Channel {

    /*logger*/
    private static final Logger logger = Logger.getLogger(Channel.class);

    /*data*/
    private int length = 10000;

    /*driver*/
    private BlockingQueue<Tuple> queue = null;

    /**
     * channel queue for tuple switcher
     * @throws Exception
     */
    public Channel() throws Exception {
        queue = new ArrayBlockingQueue<Tuple>(length);
    }

    /**
     * specify the length of the queue
     * @param len
     * @throws Exception
     */
    public Channel(int len) throws Exception {
        length = len;
        queue = new ArrayBlockingQueue<Tuple>(length);
    }

    /**
     * set length of queue
     * @param len
     */
    public void setLength(int len) {
        this.length = len;
    }

    /**
     * put tuple into queue
     * @param tuple
     * @throws Exception
     */
    public void put(Tuple tuple) throws Exception {
        if(!TupleUtils.isBlank(tuple)) {
            queue.put(tuple);
        }
    }

    /**
     * take a tuple from queue
     * @return tuple
     * @throws Exception
     */
    public Tuple take() throws Exception {
        return queue.take();
    }

    /**
     * empty
     * @return boolean
     * @throws Exception
     */
    public boolean isEmpty() throws Exception {
        return queue.isEmpty();
    }

    /**
     * clear all data of queue
     * @throws Exception
     */
    public void clear() throws Exception {
        queue.clear();
    }
}
