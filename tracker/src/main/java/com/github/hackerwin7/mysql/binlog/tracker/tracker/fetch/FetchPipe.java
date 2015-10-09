package com.github.hackerwin7.mysql.binlog.tracker.tracker.fetch;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position.MysqlRowMsgTimestampPosition;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.FetchPipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.DirectLogFetcherChannel;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogContext;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogDecoder;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlQueryExecutor;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlUpdateExecutor;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.packets.HeaderPacket;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.packets.client.BinlogDumpCommandPacket;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.utils.PacketManager;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.Pipe;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple.LogEventTuple;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * pipe mysql fetch binlog
 * Created by hp on 7/16/15.
 */
public class FetchPipe extends Pipe {

    /*data*/
    private String host = null;
    private int port = 0;
    private String usr = null;
    private String psd = null;
    private long slaveId = 0L;
    private String logFile = null;
    private long logPos = 0;
    private Charset charset = null;
    private Map<String, String> filter = null;
    private int fetchBatchCount = 1;

    /*driver*/
    private MysqlConnector logConn = null;
    private MysqlQueryExecutor logQuery = null;
    private MysqlUpdateExecutor logUpdate = null;
    /*  dbsyn driver*/
    private DirectLogFetcherChannel fetcher = null;
    private LogDecoder decoder = null;
    private LogContext context = null;

    /**
     * constructor for mysql fetcher
     * @param conf fetcher's configuration
     * @param _queue pipe's queue
     * @param cp fetcher's checkpoint, where the start position to fetch
     * @throws Exception
     */
    public FetchPipe(FetchPipeConf conf, BlockingQueue<LogEvent> _queue, MysqlRowMsgTimestampPosition cp) throws Exception {
        super(_queue);
        host = conf.getMysqlAddr();
        port = conf.getMysqlPort();
        usr = conf.getMysqlUsr();
        psd = conf.getMysqlPsd();
        slaveId = conf.getMysqlSlaveId();
        charset = conf.getMysqlCharset();
        filter = conf.getFilter();
        fetchBatchCount = conf.getFetchBatchCount();
        logFile = cp.getLogFile();
        logPos = cp.getLogPos();
    }

    /**
     * init the pipe driver
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        //init mysql driver
        logConn = new MysqlConnector(new InetSocketAddress(host, port), usr, psd);
        logConn.connect();
        logQuery = new MysqlQueryExecutor(logConn);
        logUpdate = new MysqlUpdateExecutor(logConn);
        //init dbsync driver
        logUpdate.update("set wait_timeout=9999999");
        logUpdate.update("set net_write_timeout=1800");
        logUpdate.update("set net_read_timeout=1800");
        logUpdate.update("set names 'binary'");//this will be my try to test no binary
        logUpdate.update("set @master_binlog_checksum= '@@global.binlog_checksum'");
        logUpdate.update("SET @mariadb_slave_capability='" + LogEvent.MARIA_SLAVE_CAPABILITY_MINE + "'");
        BinlogDumpCommandPacket binDmpPacket = new BinlogDumpCommandPacket();
        binDmpPacket.binlogFileName = logFile;
        binDmpPacket.binlogPosition = logPos;
        binDmpPacket.slaveServerId = slaveId;
        byte[] dmpBody = binDmpPacket.toBytes();
        HeaderPacket dmpHeader = new HeaderPacket();
        dmpHeader.setPacketBodyLength(dmpBody.length);
        dmpHeader.setPacketSequenceNumber((byte) 0x00);
        PacketManager.write(logConn.getChannel(), new ByteBuffer[]{ByteBuffer.wrap(dmpHeader.toBytes()), ByteBuffer.wrap(dmpBody)});
    }

    /**
     * prepare for fetcher pipe
     * @throws Exception
     */
    @Override
    public void prePipe() throws Exception {
        fetcher = new DirectLogFetcherChannel(logConn.getReceiveBufferSize());
        fetcher.start(logConn.getChannel());
        decoder = new LogDecoder(LogEvent.UNKNOWN_EVENT, LogEvent.ENUM_END_EVENT);
        context = new LogContext();
    }

    /**
     * fetch a event (no parse)
     * @return binlog event
     * @throws Exception
     */
    @Override
    public Tuple pipeFrom() throws Exception {
        fetcher.fetch();
        LogEvent event = decoder.decode(fetcher, context);
        Tuple tuple = new LogEventTuple(event);
        return tuple;
    }

    /**
     * pipe a batch of tuples
     * @return tupleList
     * @throws Exception
     */
    @Override
    public List<Tuple> pipeBatchFrom() throws Exception {
        List<Tuple> tuples = new ArrayList<Tuple>();
        for(int i = 1; i <= fetchBatchCount; i++) {
            fetcher.fetch();
            LogEvent event = decoder.decode(fetcher, context);
            Tuple tuple = new LogEventTuple(event);
            tuples.add(tuple);//tuple is blank will be deal in channel or queue
        }
        return tuples;
    }

    /**
     * process tuple
     * @param tuple original tuple
     * @return processed tuple
     * @throws Exception
     */
    @Override
    public Tuple dealTuple(Tuple tuple) throws Exception {
        /*no op*/
        return tuple;
    }

    /**
     * process a batch of tuples
     * @param tuples batch of
     * @return tuples
     * @throws Exception
     */
    @Override
    public List<Tuple> dealTuple(List<Tuple> tuples) throws Exception {
        /*no op*/
        return tuples;
    }

    /**
     * pipe into a queue
     * @param tuple
     * @throws Exception
     */
    @Override
    public void pipeTo(Tuple tuple) throws Exception {
        if(!tuple.isBlank()) {
            queue.put(tuple);
        }
    }

    /**
     * pipe into a queue
     * @param tuples a batch of tuples
     * @throws Exception
     */
    @Override
    public void pipeTo(List<Tuple> tuples) throws Exception {
        for (Tuple tuple : tuples) {
            if(!tuple.isBlank()) {
                queue.put(tuple);
            }
        }
    }

    /**
     * snapshot to the end of the tuple
     * @param tuple tuple
     * @throws Exception
     */
    @Override
    public void snapShot(Tuple tuple) throws Exception {
        snapTuple = tuple;
    }

    /**
     * snapshot to the end of the tuple
     * @param tuples list tuple
     * @throws Exception
     */
    @Override
    public void snapShot(List<Tuple> tuples) throws Exception {
        snapTuple = tuples.get(tuples.size() - 1);
    }

    /**
     * close the drivers
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        fetcher.close();
        logConn.disconnect();
    }
}
