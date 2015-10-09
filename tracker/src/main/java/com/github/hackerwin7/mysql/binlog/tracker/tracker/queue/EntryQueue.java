package com.github.hackerwin7.mysql.binlog.tracker.tracker.queue;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 4/27/15.
 */
public class EntryQueue extends AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(EntryQueue.class);

    public static final int                         SIG_PUT                     = 1;
    public static final int                         SIG_TAKE                    = 2;

    private BlockingQueue<EventEntry.Entry>         queue                       = null;
    private int                                     signal                      = 0;

    private EventEntry.Entry                        entry                       = null;
    private EventEntry.Entry                        ret                         = null;

    public EntryQueue(int _len) {
        queue = new LinkedBlockingQueue<EventEntry.Entry>(_len);
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _send() throws Exception {
        switch (signal) {
            case SIG_PUT:
                _put(entry);
                break;
            case SIG_TAKE:
                ret = _take();
                break;
            default:
                break;
        }
    }

    /**
     *
     * @param _entry
     * @throws Exception
     */
    public void _put(EventEntry.Entry _entry) throws Exception {
        queue.put(_entry);
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public EventEntry.Entry _take() throws Exception {
        return  queue.take();
    }

    /**
     *
     * @param _entry
     * @throws Exception
     */
    public void put(EventEntry.Entry _entry) throws Exception {
        entry = _entry;
        signal = SIG_PUT;
        send();
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public EventEntry.Entry take() throws Exception {
        signal = SIG_TAKE;
        send();
        return ret;
    }

    /**
     *
     * @return
     */
    public int size() {
        return queue.size();
    }

    /**
     * clear the blocking queue
     */
    public void clear() {
        queue.clear();
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }
}
