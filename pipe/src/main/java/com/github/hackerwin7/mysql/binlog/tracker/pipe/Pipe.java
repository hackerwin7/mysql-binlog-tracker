package com.github.hackerwin7.mysql.binlog.tracker.pipe;


import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * a basic pipe component for magpie eggs
 * Created by hp on 7/3/15.
 */
public abstract class Pipe<T> implements IPipe {
    /*logger*/
    protected Logger logger = null;

    /*driver*/

    /*data*/
    protected BlockingQueue<T> queue = null;
    protected AtomicBoolean pipingRunning = new AtomicBoolean(false);
    protected Tuple snapTuple = null;

    /**
     * constructor
     * @param _queue
     */
    public Pipe(BlockingQueue<T> _queue) {
        queue = _queue;
    }

    /**
     * start the thread, and piping constantly
     * @throws Exception
     */
    public void start() throws Exception {
        init();
        Thread pipeThd = new Thread(new Runnable() {
            public void run() {
                try {
                    pipingRunning.set(true);
                    prePipe();
                    while (pipingRunning.get()) {
                        Tuple tuple = pipeFrom();
                        tuple = dealTuple(tuple);
                        pipeTo(tuple);
                        snapShot(tuple);
                    }
                } catch (Throwable e) {
                    logger.error(logger.getName() + " error : " + e.getMessage(), e);
                } finally {
                    pipingRunning.set(false);
                }
            }
        });
        pipeThd.start();
    }

    /**
     * start the thread, and piping constantly
     * @throws Exception
     */
    public void starts() throws Exception {
        init();
        Thread pipeThd = new Thread(new Runnable() {
            public void run() {
                try {
                    pipingRunning.set(true);
                    prePipe();
                    while (pipingRunning.get()) {
                        List<Tuple> tuples = pipeBatchFrom();
                        tuples = dealTuple(tuples);
                        pipeTo(tuples);
                        snapShot(tuples);
                    }
                } catch (Throwable e) {
                    logger.error(logger.getName() + " error : " + e.getMessage(), e);
                } finally {
                    pipingRunning.set(false);
                }
            }
        });
        pipeThd.start();
    }

    /**
     * start once and exit the function
     * @throws Exception
     */
    public void startSingle() throws Exception {
        init();
        prePipe();
        Tuple tuple = pipeFrom();
        tuple = dealTuple(tuple);
        pipeTo(tuple);
        snapShot(tuple);
        close();
    }

    /**
     * sleep internal and execute loop once
     * @param millisec ms
     * @throws Exception
     */
    public void startInternal(final long millisec) throws Exception {
        init();
        Thread pipeThd = new Thread(new Runnable() {
            public void run() {
                try {
                    pipingRunning.set(true);
                    prePipe();
                    while (pipingRunning.get()) {
                        Thread.sleep(millisec);
                        Tuple tuple = pipeFrom();
                        tuple = dealTuple(tuple);
                        pipeTo(tuple);
                        snapShot(tuple);
                    }
                } catch (Throwable e) {
                    logger.error(logger.getName() + " error : " + e.getMessage(), e);
                } finally {
                    pipingRunning.set(false);
                }
            }
        });
        pipeThd.start();
    }

    /**
     * get the piping running
     * @return pipe running
     * @throws Exception
     */
    public boolean getPipingRuning() throws Exception {
        return pipingRunning.get();
    }

    /**
     * set the pipe running
     * @param is
     * @throws Exception
     */
    public void setPipingRunning(boolean is) throws Exception {
        pipingRunning.set(is);
    }

    /**
     * getter
     * @return snapshot
     */
    public Tuple getSnapTuple() {
        return snapTuple;
    }

    /**
     * getter
     * @return blocking queue
     */
    public BlockingQueue<T> getRearBlockingQueue() {
        return queue;
    }

    /**
     * stop the start thread
     * @throws Exception
     */
    public void stop() throws Exception {
        setPipingRunning(false);
    }
}
