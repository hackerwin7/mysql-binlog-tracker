package com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent;

import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.PersistPipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.Pipe;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple.RowMsgTuple;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.conf.KafkaConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.producer.KafkaProducer;
import kafka.producer.KeyedMessage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * send the row message tuples to kafka and persist the checkpoint
 * Created by hp on 7/17/15.
 */
public class PersistPipe extends Pipe {

    /*data*/
    private int persistBatchMaxMsgs = 200;
    private long persistMaxMillions = 1000;// unit : ms
    private AtomicBoolean running = new AtomicBoolean(true);
    private List<KeyedMessage<String, byte[]>> messages = new ArrayList<KeyedMessage<String, byte[]>>();

    /*  kafka data*/
    private KafkaConf kcnf = null;

    /*driver*/
    private KafkaProducer<String, byte[]> producer = null;

    /*fetching queue*/
    private BlockingQueue<RowMsgTuple> rowQueue = null;

    /**
     * constructor for persist pipe
     * @param conf : persist pipe configuration
     * @param fetchQueue : fetch RowMsgTuple from queue
     * @throws Exception
     */
    public PersistPipe(PersistPipeConf conf, BlockingQueue<RowMsgTuple> fetchQueue) throws Exception {
        super(null);
        persistBatchMaxMsgs = conf.getPersistBatchCount();
        persistMaxMillions = conf.getPersistMaxMills();
        kcnf.zkConnStr = conf.getKafkaZk();
        kcnf.zkConnRoot = conf.getKafkaZkRoot();
        kcnf.topic = conf.getKafkaTopic();
        kcnf.acks = conf.getKafkaAcks();
        rowQueue = fetchQueue;
    }

    /**
     * init the driver such as kafka
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        logger = Logger.getLogger(PersistPipe.class);
        kcnf.set("topic.metadata.refresh.interval.ms", "60000");
        kcnf.set("send.buffer.bytes", "1048576");
        kcnf.set("compression.codec", "snappy");
        kcnf.load();
        producer = new KafkaProducer<String, byte[]>(kcnf);
        producer.connect();
    }

    /**
     * prepare for persist pipe
     * @throws Exception
     */
    @Override
    public void prePipe() throws Exception {
        /*no op*/
    }

    /**
     * fetch a RowMsgTuple from queue
     * @return row message tuple
     * @throws Exception
     */
    @Override
    public Tuple pipeFrom() throws Exception {
        RowMsgTuple tuple = rowQueue.take();
        return tuple;
    }

    /**
     * fetch a list of tuples
     * @return a list of tuples
     * @throws Exception
     */
    @Override
    public List<Tuple> pipeBatchFrom() throws Exception {
        List<Tuple> tuples = new ArrayList<Tuple>();
        //if exceed the timer, then send to kafka
        Thread timer = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    Thread.sleep(persistMaxMillions);
                } catch (Exception e) {
                    logger.error(e.getMessage(), e);
                } finally {
                    running.set(false);
                }
            }
        });
        timer.start();
        int count = 0;
        messages.clear();
        fetch :
        while (running.get()) {
            int delta = 0;
            while (!rowQueue.isEmpty()) {
                RowMsgTuple tuple = rowQueue.take();
                if(tuple != null) {
                    count++;
                    delta++;
                    messages.add(new KeyedMessage<String, byte[]>(kcnf.topic, null, tuple.toBytes()));
                    //if exceed the counter, then send to kafka
                    if (count >= persistBatchMaxMsgs) {
                        break fetch;
                    }
                }
            }
            //limit the cpu
            if(delta == 0) {
                Thread.sleep(100);
            }
        }
        running.set(true);
        return tuples;
    }

    /**
     * no operation
     * @param tuple, row message tuple
     * @return RowMsgTuple
     * @throws Exception
     */
    @Deprecated
    @Override
    public Tuple dealTuple(Tuple tuple) throws Exception {
        /*no op*/
        return tuple;
    }

    /**
     * no operation
     * @param tuples, row message tuples
     * @return List of RowMsgTuples
     * @throws Exception
     */
    @Override
    public List<Tuple> dealTuple(List<Tuple> tuples) throws Exception {
        /*no op*/
        return tuples;
    }

    /**
     * send to kafka
     * @param tuple, RowMsgTuple
     * @throws Exception
     */
    @Override
    public void pipeTo(Tuple tuple) throws Exception {
        producer.send(new KeyedMessage<String, byte[]>(kcnf.topic, null, tuple.toBytes()));
    }

    /**
     * send to kafka
     * @param tuples, a list of tuple
     * @throws Exception
     */
    @Override
    public void pipeTo(List<Tuple> tuples) throws Exception {
        producer.send(messages);
    }

    /**
     * it is not available for snapshot
     * @param tuple
     * @throws Exception
     */
    @Deprecated
    @Override
    public void snapShot(Tuple tuple) throws Exception {
        snapTuple = tuple;
    }

    /**
     * find the latest of the end batch of the Row message to persist
     * @param tuples, a list of tuple
     * @throws Exception
     */
    @Override
    public void snapShot(List<Tuple> tuples) throws Exception {
        snapTuple = tuples.get(tuples.size() - 1);
    }

    /**
     * close the kafka producer
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        producer.close();
    }
}
