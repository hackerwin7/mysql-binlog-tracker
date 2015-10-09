package com.github.hackerwin7.mysql.binlog.tracker.monitor;

/**
 * central server for monitor
 * save monitor message into the collection
 * Created by hp on 7/24/15.
 */
public abstract class MonitorServer {
//    /*logger*/
//    private Logger logger = Logger.getLogger(MonitorServer.class);
//
//    /*message lib, blocking queue data*/
//    private BlockingQueue<Message> msgQueue = null;
//
//    /*running info*/
//    private AtomicBoolean running = new AtomicBoolean(false);
//
//    /**
//     * server constructor
//     * @throws Exception
//     */
//    public MonitorServer() throws Exception {
//
//    }
//
//    /**
//     * start the server
//     * @throws Exception
//     */
//    public void start() throws Exception {
//        Thread th = new Thread(new Runnable() {
//            @Override
//            public void run() {
//                running.set(true);
//                while (running.get()) {
//                    Message msg = msgQueue.take();
//
//                }
//            }
//        });
//        th.start();
//    }
//
//    /**
//     * push the message out from server
//     * @param msg, monitor message
//     * @throws Exception
//     */
//    protected abstract void show(Message msg) throws Exception;
//
//    /**
//     * put message into queue
//     * @param msg, message
//     * @throws Exception
//     */
//    public void push(Message msg) throws Exception {
//        msgQueue.put(msg);
//    }
}
