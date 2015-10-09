package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.consumer.high;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.conf.KafkaConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.consumer.stream.KafkaStreamConsumer;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.queue.KafkaBlockingQueue;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Created by hp on 4/22/15.
 */
public class KafkaHighConsumer extends AbstractDriver {

    private Logger                                  logger                      = Logger.getLogger(KafkaHighConsumer.class);

    private KafkaConf conf                        = null;
    private KafkaBlockingQueue queue                       = null;
    private ConsumerConnector                       consumer                    = null;
    private int                                     threadCnt                   = 1;
    private ExecutorService                         executor                    = null;

    /**
     *
     * @param _conf
     * @param _queue
     */
    public KafkaHighConsumer(KafkaConf _conf, KafkaBlockingQueue _queue) {
        conf = new KafkaConf(_conf);
        queue = _queue;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _connect() throws Exception {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(new ConsumerConfig(conf.props));
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(conf.topic, threadCnt);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(conf.topic);
        executor = Executors.newFixedThreadPool(threadCnt);
        int threadNo = 0;
        for(final KafkaStream stream : streams) {
            executor.submit(new KafkaStreamConsumer(stream, threadNo, queue));
            threadNo++;
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void disconnect() throws Exception {
        super.disconnect();
        if(consumer != null) consumer.shutdown();
        if(executor != null) executor.shutdown();
        try {
            if (executor.awaitTermination(CommonConstants.THREAD_AWAT_TIME, TimeUnit.MILLISECONDS)) {
                logger.info("high level consumer thread poll shutdown successfully......");
            } else {
                logger.info("time out waiting for consumer threads to shutdown, exiting uncleanly");
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    /**
     *
     * @return
     */
    public KafkaBlockingQueue getQueue() {
        return queue;
    }

    /**
     *
     * @param threadCnt
     */
    public void setThreadCnt(int threadCnt) {
        if(threadCnt <= 0) {
            logger.info("thread count is 0 or negative, use the default 1.");
            threadCnt = 1;
        }
        if(threadCnt > CommonConstants.THREAD_COUNT_LIMIT) {
            logger.info("thread count is exceed the limit , set the default count limit " + CommonConstants.THREAD_COUNT_LIMIT);
            threadCnt = CommonConstants.THREAD_COUNT_LIMIT;
        }
        this.threadCnt = threadCnt;
    }

}
