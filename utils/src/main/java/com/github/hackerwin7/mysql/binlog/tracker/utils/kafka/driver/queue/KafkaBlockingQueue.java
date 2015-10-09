package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.queue;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.metadata.KafkaMessage;
import org.apache.log4j.Logger;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Created by hp on 4/22/15.
 */
public class KafkaBlockingQueue extends AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(KafkaBlockingQueue.class);

    public static final int                         SIG_PUT                     = 1;
    public static final int                         SIG_TAKE                    = 2;

    private BlockingQueue<KafkaMessage>             msgQueue                    = null;
    private int                                     signal                      = 0;

    private KafkaMessage                            km                          = null;
    private KafkaMessage                            ret                         = null;

    public KafkaBlockingQueue(int _len) {
        msgQueue = new LinkedBlockingQueue<KafkaMessage>(_len);
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _send() throws Exception {
        switch (signal) {
            case SIG_PUT:
                _put(km);
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
     * @param _km
     * @throws Exception
     */
    public void _put(KafkaMessage _km) throws Exception {
        msgQueue.put(km);
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public KafkaMessage _take() throws Exception {
        return  msgQueue.take();
    }

    /**
     *
     * @param _km
     * @throws Exception
     */
    public void put(KafkaMessage _km) throws Exception {
        km = _km;
        signal = SIG_PUT;
        send();
    }

    /**
     *
     * @return
     * @throws Exception
     */
    public KafkaMessage take() throws Exception {
        signal = SIG_TAKE;
        send();
        return ret;
    }

    /**
     *
     * @return
     */
    public int size() {
        return msgQueue.size();
    }

    /**
     * clear the blocking queue
     */
    public void clear() {
        msgQueue.clear();
    }

}
