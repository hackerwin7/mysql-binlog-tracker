package com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.driver.producer;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.utils.kafka.conf.KafkaConf;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.List;

/**
 * Created by hp on 4/21/15.
 */
public class KafkaProducer<K, V> extends AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(KafkaProducer.class);

    private KafkaConf conf                        = null;
    private Producer<K, V>                          producer                    = null;

    private int                                     signal                      = 0;

    public static final int                         SIG_SEND_MSG                = 1;
    public static final int                         SIG_SEND_MSGS               = 2;

    public int                                      retInt                      = 0;//1 is send error

    public KeyedMessage<K, V>                       msg                         = null;
    public List<KeyedMessage<K, V>>                 msgs                        = null;

    /**
     *
     * @param _cnf
     */
    public KafkaProducer(KafkaConf _cnf) {
        conf = new KafkaConf(_cnf);
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _connect() throws Exception {
        producer = new Producer<K, V>(new ProducerConfig(conf.props));
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void disconnect() throws Exception {
        super.disconnect();
        producer.close();
    }

    /**
     *
     * @return
     * @throws Exception
     */
    @Override
    public boolean isConnected() throws Exception {
        return super.isConnected();
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void _send() throws Exception {
        switch (signal) {
            case SIG_SEND_MSG:
                _send(msg);
                break;
            case SIG_SEND_MSGS:
                _send(msgs);
                break;
            default:
                //no op
                break;
        }
    }

    /**
     *
     * @param _msg
     */
    public void _send(KeyedMessage<K, V> _msg) throws Exception {
        producer.send(_msg);
    }

    /**
     *
     * @param _msgs
     */
    public void _send(List<KeyedMessage<K, V>> _msgs) throws Exception {
        producer.send(_msgs);
    }

    public void send(KeyedMessage<K, V> _msg) throws Exception {
        msg = _msg;
        signal = SIG_SEND_MSG;
        send();
    }

    public void send(List<KeyedMessage<K, V>> _msgs) throws Exception {
        msgs = _msgs;
        signal = SIG_SEND_MSGS;
        send();
    }
}
