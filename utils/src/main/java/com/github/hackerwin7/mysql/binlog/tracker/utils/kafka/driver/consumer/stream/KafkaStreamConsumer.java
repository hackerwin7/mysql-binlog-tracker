package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.consumer.stream;

import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.queue.KafkaBlockingQueue;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.metadata.KafkaMessage;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.apache.log4j.Logger;

/**
 * Created by hp on 4/22/15.
 */
public class KafkaStreamConsumer implements Runnable {
    private Logger                                  logger                      = Logger.getLogger(KafkaStreamConsumer.class);

    private KafkaStream                             kstream                     = null;
    private int                                     tno                         = 1;//thread number not count
    private KafkaBlockingQueue queue                       = null;

    /**
     *
     * @param _stream
     * @param _num
     */
    public KafkaStreamConsumer(KafkaStream _stream, int _num, KafkaBlockingQueue _queue) {
        kstream = _stream;
        tno = _num;
        queue = _queue;
    }

    /**
     * thread run function
     */
    @Override
    public void run() {
        ConsumerIterator<byte[], byte[]> it = kstream.iterator();
        while (it.hasNext()) {
            try {
                MessageAndMetadata<byte[], byte[]> curMeta = it.next();
                byte[] data = curMeta.message();
                byte[] key = curMeta.key();
                int partition = curMeta.partition();
                long offset = curMeta.offset();
                long pullTime = 0;
                KafkaMessage km = new KafkaMessage();
                km.msg = data;
                km.key = key;
                km.partition = partition;
                km.offset = offset;
                km.pullTime = pullTime;
                queue.put(km);
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
        }
    }
}
