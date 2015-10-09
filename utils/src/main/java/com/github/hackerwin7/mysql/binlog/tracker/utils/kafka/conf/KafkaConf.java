package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.conf;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.CommonConstants;
import com.github.hackerwin7.mysql.binlog.tracker.commons.constants.KafkaConstants;
import com.github.hackerwin7.mysql.binlog.tracker.utils.convert.ConvertUtils;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver.ZkExecutor;
import net.sf.json.JSONObject;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.Properties;

/**
 * Created by hp on 4/21/15.
 */
public class KafkaConf extends AbstractConf {
    private Logger                                  logger                      = Logger.getLogger(KafkaConf.class);

    public String                                   zkConnStr                   = "127.0.0.1:2181";
    public String                                   zkConnRoot                  = "/kafka";
    public String                                   acks                        = "-1";
    public int                                      partiton                    = 0;
    public String                                   topic                       = "topic";
    public String                                   clientName                  = "dfaewfwt";
    public long                                     readOffset                  = Long.MAX_VALUE;

    public Properties                               props                       = new Properties();


    /**
     * default constructor
     */
    public KafkaConf() {

    }

    public KafkaConf(KafkaConf _cnf) {
        zkConnStr = _cnf.zkConnStr;
        zkConnRoot = _cnf.zkConnRoot;
        acks = _cnf.acks;
        partiton = _cnf.partiton;
        topic = _cnf.topic;
        clientName = _cnf.clientName;
        props = new Properties(_cnf.props);
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void load() throws Exception {
        super.load();
        ZookeeperConf zcnf = new ZookeeperConf(zkConnStr);
        ZkExecutor zk = new ZkExecutor(zcnf);
        zk.connect();
        List<String> brokers = zk.getChildren(zkConnRoot + CommonConstants.ZK_BROKERS_IDS);
        logger.info("load kafka brokers : " + brokers.toString());
        String brokerStr = "";
        for(String broker : brokers) {
            String jsonStr = zk.get(zkConnRoot + CommonConstants.ZK_BROKERS_IDS + "/" + broker);
            JSONObject jdata = ConvertUtils.String2Json(jsonStr);
            String host = jdata.getString("host");
            int port = jdata.getInt("port");
            brokerStr += host + ":" + port + ",";
        }
        brokerStr = brokerStr.substring(0, brokerStr.lastIndexOf(","));
        if(!props.containsKey(KafkaConstants.KAFKA_PROP_BROKERS)) {
            props.put(KafkaConstants.KAFKA_PROP_BROKERS, brokerStr);
        }
        if(!props.containsKey(KafkaConstants.KAFKA_PROP_SERIALIZER)) {
            props.put(KafkaConstants.KAFKA_PROP_SERIALIZER, "kafka.serializer.DefaultEncoder");
        }
        if(!props.containsKey(KafkaConstants.KAFKA_PROP_KEY_SERIALIZER)) {
            props.put(KafkaConstants.KAFKA_PROP_KEY_SERIALIZER, "kafka.serializer.StringEncoder");
        }
        if(!props.containsKey(KafkaConstants.KAFKA_PROP_PARTITIONER)) {
            props.put(KafkaConstants.KAFKA_PROP_PARTITIONER, "kafka.producer.DefaultPartitioner");
        }
        if(!props.containsKey(KafkaConstants.KAFKA_PROP_ACKS)) {
            props.put(KafkaConstants.KAFKA_PROP_ACKS, acks);
        }
        zk.disconnect();
        zk.close();
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void clear() throws Exception {
        super.clear();
        if(props != null) {
            props.clear();
        } else {
            props = new Properties();
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void statis() throws Exception {
        logger.info("kafka conf:");
        logger.info("zookeeper connection string:" + zkConnStr);
        logger.info("zookeeper connection root:" + zkConnRoot);
        logger.info("kafka acks:" + acks);
        logger.info("kafka partitions:" + partiton);
        logger.info("kafka topic:" + topic);
        logger.info("kafka properties:" + props.toString());
    }

    /**
     *
     * @param key
     * @param value
     */
    public void set(String key, String value) {
        props.setProperty(key, value);
    }
}
