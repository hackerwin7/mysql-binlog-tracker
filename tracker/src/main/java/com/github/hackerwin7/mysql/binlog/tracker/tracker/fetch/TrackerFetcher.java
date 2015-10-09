package com.github.hackerwin7.mysql.binlog.tracker.tracker.fetch;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractConf;
import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogContext;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogDecoder;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.packets.server.ResultSetPacket;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.parse.LogEventConvert;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.queue.EntryQueue;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.PositionOffset;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver.ZkExecutor;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * Created by hp on 4/27/15.
 */
public class TrackerFetcher extends AbstractDriver implements Runnable {
    private Logger                                  logger                      = Logger.getLogger(TrackerFetcher.class);

    /*data set*/
    private AbstractConf                            conf                        = null;
    private EntryQueue                              queue                       = null;

    /*binlog parse*/
    private DirectLogFetcherChannel                 fetcher                     = null;
    private LogDecoder                              decoder                     = null;
    private LogContext                              context                     = null;
    private LogEvent                                event                       = null;

    /*mysql driver*/
    private MysqlConnector                          logConn                     = null;
    private MysqlConnector                          tabConn                     = null;
    private MysqlConnector                          reaConn                     = null;
    private MysqlQueryExecutor                      logExec                     = null;
    private MysqlUpdateExecutor                     updExec                     = null;
    private MysqlQueryExecutor                      reaExec                     = null;

    /*mysql meta*/
    private TableMetaCache tabMeCa                     = null;

    /*event parse*/
    LogEventConvert convert                     = null;


    /*zookeeper find position*/
    private ZkExecutor                              zkExecutor                  = null;

    /*positions*/
    private PositionOffset                          pos                         = null;


    public TrackerFetcher(AbstractConf _conf, EntryQueue _queue) {
        conf = _conf;
        queue = _queue;
    }

    /**
     * opstat() -> connect
     * @throws Exception
     */
    @Override
    public void _connect() throws Exception {
        pos = new PositionOffset();
        pos.setLogfile(conf.logfile);
        pos.setOffset(conf.offset);
        pos.setBid(conf.batchId);
        pos.setIid(conf.inId);
        pos.setMid(conf.mid);
        zkExecutor = new ZkExecutor(new ZookeeperConf(conf.offsetZk));
        zkExecutor.connect();
        String address = conf.mysqlAddr;
        int port = conf.mysqlPort;
        String usr = conf.mysqlUsr;
        String psd = conf.mysqlPsd;
        long slaveId = conf.mysqlSlaveId;
        logConn = new MysqlConnector(new InetSocketAddress(address, port), usr, psd);
        tabConn = new MysqlConnector(new InetSocketAddress(address, port), usr, psd);
        reaConn = new MysqlConnector(new InetSocketAddress(address, port), usr, psd);
        logConn.connect();
        tabConn.connect();
        reaConn.connect();
        logExec = new MysqlQueryExecutor(logConn);
        updExec = new MysqlUpdateExecutor(logConn);
        reaExec = new MysqlQueryExecutor(reaConn);
        tabMeCa = new TableMetaCache(tabConn);
        convert = new LogEventConvert();
        convert.setTableMetaCache(tabMeCa);
        convert.setCharset(conf.mysqlCharset);
        convert.setFilterMap(conf.filterMap);
        convert.setSenseMap(conf.senseMap);
        findExternalPos();
        //binlog dump thread configuration
        logger.info("set the binlog configuration for the binlog dump");
        updExec.update("set wait_timeout=9999999");
        updExec.update("set net_write_timeout=1800");
        updExec.update("set net_read_timeout=1800");
        updExec.update("set names 'binary'");//this will be my try to test no binary
        updExec.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
        updExec.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
        //send binlog dump packet and mysql will establish a binlog dump thread
        logger.info("send the binlog dump packet to mysql , let mysql set up a binlog dump thread in mysql");
        BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
        binDmpPacket.binlogFileName = pos.getLogfile();
        binDmpPacket.binlogPosition = pos.getOffset();
        binDmpPacket.slaveServerId = slaveId;
        byte[] dmpBody = binDmpPacket.toBytes();
        HeaderPacket dmpHeader = new HeaderPacket();
        dmpHeader.setPacketBodyLength(dmpBody.length);
        dmpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(logConn.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
        //initialize the mysql.dbsync to fetch the binlog data
        fetcher = new DirectLogFetcherChannel(logConn.getReceiveBufferSize());
        fetcher.start(logConn.getChannel());
        decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        context = new LogContext();
        logger.info("------------------->fetcher load position : " + pos.getPersistent());
    }

    /**
     *
     * @throws Exception
     */
    private void findExternalPos() throws Exception {
        findPosFromZk();
    }

    /**
     *
     * @param executor
     */
    private void findPosFromMysqlNow(MysqlQueryExecutor executor) {
        if(executor == null) return;
        try {
            ResultSetPacket packet = executor.query("show master status");
            List<String> fields = packet.getFieldValues();
            if(CollectionUtils.isEmpty(fields)) {
                throw new Exception("show master status failed");
            }
            pos.setLogfile(fields.get(0));
            pos.setOffset(Long.valueOf(fields.get(1)));
        } catch (Exception e) {
            logger.error("show master status error, " + e.getMessage(), e);
        }
    }

    /**
     * find position form zookeeper
     */
    private void findPosFromZk() {
        logger.info("finding position......");
        try {
            String zkPos = conf.offsetPersis + "/" + conf.jobId;
            String getStr = zkExecutor.get(zkPos);
            if(getStr == null || getStr.equals("")) {//zk had no position
                if(pos.getOffset() <= 0) {
                    logger.info("find mysql show master status......");
                    findPosFromMysqlNow(logExec);
                    pos.setBid(0);
                    pos.setIid(0);
                    pos.setMid(0);
                } else {
                    logger.info("find mysql position from configuration......");
                }
            } else {//zk had position
                String[] ss = getStr.split(":");
                if (ss.length != 5) {
                    zkExecutor.delete(zkPos);
                    logger.error("zk position format is error, it would load latest offset to fetch......");
                } else {
                    logger.info("find zk position......");
                    pos.setLogfile(ss[0]);
                    pos.setOffset(Long.valueOf(ss[1]));
                    pos.setBid(Long.valueOf(ss[2]));
                    pos.setIid(Long.valueOf(ss[3]));
                    pos.setMid(Long.valueOf(ss[4]));
                }
            }
            logger.info("start position : " + pos.getLogfile() + " : " + pos.getOffset() +
                    " : " + pos.getBid() +
                    " : " + pos.getIid() +
                    " : " + pos.getMid());
        } catch (Exception e) {
            logger.error("zk client error : " + e.getMessage(), e);
        }
    }

    /**
     * run the dump
     */
    @Override
    public void run() {
        try {
            dump();
        } catch (Exception e) {
            logger.error("fetch thread encounter error, " + e.getMessage(), e);
            status = RELOAD;
            throw new RuntimeException("reload the job");
        }
    }

    /**
     * dump the binlog event and parser to entry
     * @throws Exception
     */
    public void dump() throws Exception {
        while (fetcher.fetch()) {
            event = decoder.decode(fetcher, context);
            if(event == null) {
                logger.info("fetch the event is null......");
                continue;
            }
            EventEntry.Entry entry = convert.parse(event);
            if(entry == null) {
                logger.info("parse the entry is null......");
                continue;
            }
            queue.put(entry);
        }
    }

    /**
     *
     * @throws Exception
     */
    @Override
    public void disconnect() throws Exception {
        super.disconnect();
        fetcher.close();
        logConn.disconnect();
        tabConn.disconnect();
        reaConn.disconnect();
        zkExecutor.disconnect();
    }

    public PositionOffset getPos() {
        return pos;
    }
}
