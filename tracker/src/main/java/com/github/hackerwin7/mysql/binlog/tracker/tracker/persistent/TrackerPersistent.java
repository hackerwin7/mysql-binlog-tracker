package com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.conf.KafkaConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.producer.KafkaProducer;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver.ZkExecutor;
import kafka.producer.KeyedMessage;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by hp on 4/28/15.
 */
public class TrackerPersistent extends AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(TrackerPersistent.class);

    /*data set*/
    private AbstractConf                            conf                        = null;

    /*position persistence*/
    private ZkExecutor                              zkExecutor                  = null;

    /*kafka send*/
    private KafkaProducer<String, byte[]>           producer                    = null;

    public TrackerPersistent(AbstractConf _conf) {
        conf = _conf;
    }

    /**
     * details in connect
     * @throws Exception
     */
    public void _connect() throws Exception {
        zkExecutor = new ZkExecutor(new ZookeeperConf(conf.offsetZk));
        zkExecutor.connect();
        KafkaConf kafkaConf = createKafkaDataConf();
        producer = new KafkaProducer<String, byte[]>(kafkaConf);
    }

    /**
     * create the abstract conf to kafka conf
     * @return
     */
    private KafkaConf createKafkaDataConf() throws Exception {
        KafkaConf kafkaConf = new KafkaConf();
        kafkaConf.zkConnStr = conf.kafkaZk;
        kafkaConf.zkConnRoot = conf.kafkaRoot;
        kafkaConf.acks = conf.kafkaAcks;
        kafkaConf.topic = conf.kafkaTopic;
        kafkaConf.load();
        return kafkaConf;
    }

    /**
     *
     * @param msgs
     * @throws Exception
     */
    public void persistent(List<KeyedMessage<String, byte[]>> msgs) throws Exception {
        if(msgs .size() > 0) {
            producer.send(msgs);
        }
    }

    /**
     *
     * @param msg
     * @throws Exception
     */
    public void persistent(KeyedMessage<String, byte[]> msg) throws Exception {
        if(msg != null) {
            producer.send(msg);
        }
    }
}
