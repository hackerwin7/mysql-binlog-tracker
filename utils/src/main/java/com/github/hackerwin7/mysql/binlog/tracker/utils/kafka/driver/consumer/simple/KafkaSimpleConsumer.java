package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.consumer.simple;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.KafkaConstants;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.conf.KafkaConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.queue.KafkaBlockingQueue;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.metadata.KafkaMessage;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.ErrorMapping;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.message.MessageAndOffset;
import org.apache.log4j.Logger;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 4/23/15.
 */
public class KafkaSimpleConsumer extends AbstractDriver implements Runnable {
    private Logger                                  logger                      = Logger.getLogger(KafkaSimpleConsumer.class);

    private KafkaConf conf                        = null;
    private Map<String, SimpleConsumer>             consumerPool                = null;
    private KafkaBlockingQueue queue                       = null;
    private String                                  leadBroker                  = null;

    /**
     *
     * @param _conf
     */
    public KafkaSimpleConsumer(KafkaConf _conf, KafkaBlockingQueue _queue) {
        conf = new KafkaConf(_conf);
        consumerPool = new HashMap<String, SimpleConsumer>();
        queue = _queue;
    }

    /**
     *
     * @param broker
     * @param topic
     * @param partition
     * @return
     * @throws Exception
     */
    private SimpleConsumer createSimpleConsumer(String broker, String topic, int partition) throws Exception {
        String key = broker + "#" + topic + "#" + partition;
        if(!consumerPool.containsKey(key)) {
            String[] brokerPort = broker.split(":");
            String brokerStr = brokerPort[0];
            String clientName = conf.clientName;
            int port = Integer.valueOf(brokerPort[1]);
            SimpleConsumer consumer = new SimpleConsumer(brokerStr, port, 100000, 64 * 1024, clientName);
            consumerPool.put(key, consumer);
        }
        return consumerPool.get(key);
    }

    private void releaseSimpleConsumer(String broker, String topic, int partition) throws Exception {
        String key = broker + "#" + topic + "#" + partition;
        if(consumerPool.containsKey(key)) {
            SimpleConsumer consumer = consumerPool.get(key);
            if(consumer != null) {
                consumer.close();
            }
        }
        consumerPool.remove(key);
    }

    /**
     *
     * @return
     * @throws Exception
     */
    private PartitionMetadata findLeader() throws Exception {
        PartitionMetadata returnMetaData = null;
        String[] brokers = conf.props.getProperty(KafkaConstants.KAFKA_PROP_BROKERS).split(",");
        String topic = conf.topic;
        int partition = conf.partiton;
        loop:
        for(String broker : brokers) {
            SimpleConsumer consumer = createSimpleConsumer(broker, topic, partition);
            List<String> topics = Collections.singletonList(topic);
            TopicMetadataRequest request = new TopicMetadataRequest(topics);
            TopicMetadataResponse response = consumer.send(request);
            List<TopicMetadata> metadatas = response.topicsMetadata();
            for(TopicMetadata item : metadatas) {
                for(PartitionMetadata part : item.partitionsMetadata()) {
                    if(part.partitionId() == partition) {
                        returnMetaData = part;
                        break loop;
                    }
                }
            }
        }
        if(returnMetaData != null) {
            leadBroker = returnMetaData.leader().host() + ":" + returnMetaData.leader().port();
        }
        return returnMetaData;
    }

    /**
     *
     * @return
     * @throws Exception
     */
    private long getLatestOffset() throws Exception {
        String topic = conf.topic;
        int partition = conf.partiton;
        SimpleConsumer consumer = createSimpleConsumer(leadBroker, topic, partition);
        long whichTime = kafka.api.OffsetRequest.LatestTime();
        String clientName = conf.clientName;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if(response.hasError()) {
            logger.error("getLastOffset error : Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return Long.MAX_VALUE;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    /**
     *
     * @return
     * @throws Exception
     */
    private long getEarliestOffset() throws Exception {
        String topic = conf.topic;
        int partition = conf.partiton;
        SimpleConsumer consumer = createSimpleConsumer(leadBroker, topic, partition);
        long whichTime = kafka.api.OffsetRequest.EarliestTime();
        String clientName = conf.clientName;
        TopicAndPartition topicAndPartition = new TopicAndPartition(topic, partition);
        Map<TopicAndPartition, PartitionOffsetRequestInfo> requestInfoMap = new HashMap<TopicAndPartition, PartitionOffsetRequestInfo>();
        requestInfoMap.put(topicAndPartition, new PartitionOffsetRequestInfo(whichTime, 1));
        OffsetRequest request = new OffsetRequest(requestInfoMap, kafka.api.OffsetRequest.CurrentVersion(), clientName);
        OffsetResponse response = consumer.getOffsetsBefore(request);
        if(response.hasError()) {
            logger.error("getLastOffset error : Error fetching data Offset Data the Broker. Reason: " + response.errorCode(topic, partition));
            return Long.MAX_VALUE;
        }
        long[] offsets = response.offsets(topic, partition);
        return offsets[0];
    }

    /**
     *
     * @return
     * @throws Exception
     */
    private String findNewLeader() throws Exception {
        String ss[] = leadBroker.split(":");
        String oldBroker = ss[0];
        int oldPort = Integer.valueOf(ss[1]);
        for(int i = 0; i < 3; i++) {
            boolean goToSleep = false;
            PartitionMetadata metadata = findLeader();
            if(metadata == null) {
                goToSleep = true;
            } else if(metadata.leader() == null) {
                goToSleep = true;
            } else if(oldBroker.equalsIgnoreCase(metadata.leader().host()) && i == 0) {
                goToSleep= true;
            } else {
                leadBroker = metadata.leader().host() + ":" + metadata.leader().port();
                return metadata.leader().host() + ":" + metadata.leader().port();
            }
            if(goToSleep) {
                delay(1);
            }
        }
        throw new Exception("Unable to find new leader after Broker failure. Exiting...");
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _connect() throws Exception {
        if(leadBroker == null) {
            findLeader();
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void disconnect() throws Exception {
        super.disconnect();
        leadBroker = null;
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _pull() throws Exception {
        Thread thread = new Thread(this);
        thread.start();
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void run() {
        try {
            PartitionMetadata metadata = findLeader();
            if(metadata == null) {
                logger.error("Can't find metadata for Topic and Partition. Exiting......");
                status = RELOAD;
                throw new RuntimeException("simple consumer encounter error!!!");
            }
            if(metadata.leader() == null) {
                logger.error("Can't find Leader for Topic and Partition. Exiting......");
                status = RELOAD;
                throw new RuntimeException("simple consumer encounter error!!!");
            }
            String leader = metadata.leader().host();
            int port = metadata.leader().port();
            String leaderBrokerMeta = leader + ":" + port;
            String clientName = conf.clientName;
            String topic = conf.topic;
            int partition = conf.partiton;
            long readOffset = conf.readOffset;
            int numErrors = 0;
            SimpleConsumer consumer = createSimpleConsumer(leaderBrokerMeta, topic, partition);
            while (status == RUNNING) {
                if(consumer == null) {
                    consumer = createSimpleConsumer(leaderBrokerMeta, topic, partition);
                }
                FetchRequest req = new FetchRequestBuilder()
                        .clientId(clientName)
                        .addFetch(topic, partition, readOffset, 100000)
                        .build();
                FetchResponse res = consumer.fetch(req);
                if(res.hasError()) {
                    numErrors++;
                    short code = res.errorCode(topic, partition);
                    logger.error("Error fetching data from the Broker:" + leaderBrokerMeta + " Reason: " + code);
                    if(numErrors > 5) {
                        return;
                    }
                    if(code == ErrorMapping.OffsetOutOfRangeCode()) {
                        long startOffset = getEarliestOffset();
                        long endOffset = getLatestOffset();
                        if(readOffset <= startOffset) {
                            readOffset = startOffset;
                        }
                        if(readOffset >= endOffset) {
                            readOffset = endOffset;
                        }
                    }
                    leaderBrokerMeta = findNewLeader();
                    consumer = null;
                } else {
                    numErrors = 0;
                    long msgRead = 0;
                    for(MessageAndOffset messageAndOffset : res.messageSet(topic, partition)) {
                        long currentOffset = messageAndOffset.offset();
                        if(currentOffset < readOffset) {
                            logger.error("Found an old offset: " + currentOffset + " Expecting: " + readOffset);
                            continue;
                        }
                        readOffset = messageAndOffset.nextOffset();
                        ByteBuffer payload = messageAndOffset.message().payload();
                        byte[] bytes = new byte[payload.limit()];
                        payload.get(bytes);
                        KafkaMessage msg = new KafkaMessage();
                        msg.msg = bytes;
                        msg.offset = readOffset;//next offset
                        queue.put(msg);
                        msgRead++;
                    }
                }
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }
}
