package com.github.hackerwin7.mysql.binlog.tracker.tracker.parse;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position.MysqlRowMsgTimestampPosition;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.ParsePipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.driver.MysqlConnector;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.Pipe;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple.LogEventTuple;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple.RowMsgTuple;

import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;

/**
 * parse the event to entry and divide the entry to row message
 * Created by hp on 7/17/15.
 */
public class ParsePipe extends Pipe {

    /*data*/
    private String host = null;
    private int port = 0;
    private String usr = null;
    private String psd = null;
    private int parseBatchCount = 1;
    private Charset charset = null;
    private Map<String, String> filter = null;

    /*pos*/
    private EventEntry.RowMsgPos lastPos = null;
    private EventEntry.RowMsgPos curPos = null;
    private MysqlRowMsgTimestampPosition loadPos = null;

    /*fetch mode*/
    private enum FetchMode {
        FILTER, FETCH
    }
    private FetchMode fetchMode = FetchMode.FILTER;

    /*unique id*/
    private long uniqueId = 0;

    /*driver*/
    private MysqlConnector tableConn = null;
    private TableMetaCache tableMeta = null;

    /*parser, converter*/
    private LogEventConvert convert = null;

    /*fetching queue*/
    BlockingQueue<LogEventTuple> eventQueue = null;

    /**
     * constructor for parse
     * @param conf parseConf
     * @param _queue blocking queue
     * @param fetchQueue fetching the logEvent to parse
     * @throws Exception
     */
    public ParsePipe(ParsePipeConf conf, BlockingQueue<EventEntry.RowMsg> _queue, BlockingQueue<LogEventTuple> fetchQueue, MysqlRowMsgTimestampPosition cp) throws Exception {
        super(_queue);
        parseBatchCount = conf.getParseBatchCount();
        eventQueue = fetchQueue;
        loadPos = cp;
    }

    /**
     * init the driver
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        tableConn = new MysqlConnector(new InetSocketAddress(host, port), usr, psd);
        tableConn.connect();
        tableMeta = new TableMetaCache(tableConn);
    }

    /**
     * prepare for the fetch event pipe
     * @throws Exception
     */
    @Override
    public void prePipe() throws Exception {
        convert = new LogEventConvert();
        convert.setTableMetaCache(tableMeta);
        convert.setCharset(charset);
        convert.setFilterMap(filter);
        uniqueId = loadPos.getUniqueId() + 1; // next unique id
    }

    /**
     * take a tuple from event queue
     * @return event tuple
     * @throws Exception
     */
    @Override
    public Tuple pipeFrom() throws Exception {
        LogEventTuple eventTuple = eventQueue.take();
        return eventTuple;
    }

    /**
     * take a batch of tuples from event queue
     * @return event tuple list
     * @throws Exception
     */
    @Override
    public List<Tuple> pipeBatchFrom() throws Exception {
        List<Tuple> tuples = new ArrayList<Tuple>();
        for(int i = 1; i <= parseBatchCount; i++) {
            LogEventTuple  eventTuple = eventQueue.take();
            tuples.add(eventTuple);
        }
        return tuples;
    }

    /**
     * parse and divide
     * @param tuple LogEventTuple
     * @return null
     * @throws Exception
     */
    @Deprecated
    @Override
    public Tuple dealTuple(Tuple tuple) throws Exception {
        /*no op*/
        return null;
    }

    /**
     * parse and divide
     * @param tuples a list of LogEventTuple
     * @return a list of RowMsgTuple
     * @throws Exception
     */
    @Override
    public List<Tuple> dealTuple(List<Tuple> tuples) throws Exception {
        List<Tuple> tupleList = new ArrayList<Tuple>();
        //position var
        long rowId = 0;
        long timestampId = 0;
        for(Tuple tuple : tuples) {
            //parse
            LogEventTuple eventTuple = (LogEventTuple) tuple;
            LogEvent event = eventTuple.getEvent();
            EventEntry.Entry entry = convert.parse(event);
            //divide
            long fetchTime = System.currentTimeMillis();
            if(entry != null && entry.getHeader().getServerId() == loadPos.getServerId()) {
                EventEntry.RowChange rowChange = EventEntry.RowChange.parseFrom(entry.getStoreValue());
                //reset row id, it is in batch id into the event
                rowId = 0;
                //reset or add timestampId, it is in batch id into the same timestamp event
                if(fetchMode == FetchMode.FETCH) {
                    if(lastPos.getTimestamp() == entry.getHeader().getExecuteTime()) {
                        timestampId = lastPos.getTimestampId() + 1;
                    } else {
                        timestampId = 0;
                    }
                } else {// fetch the first reload checkpoint's event
                    timestampId = loadPos.getTimestampId();
                }
                for(EventEntry.RowData rowData : rowChange.getRowDatasList()) {
                    if(fetchMode == FetchMode.FILTER) { //filter mode
                        if(rowId <= loadPos.getRowId()) { // logFile+logPos must map to unique event
                            if(rowId == loadPos.getRowId()) {
                                fetchMode = FetchMode.FETCH;
                                //record last pos
                                lastPos = EventEntry.RowMsgPos.newBuilder()
                                        .setLogFile(loadPos.getLogFile())
                                        .setLogPos(loadPos.getLogPos())
                                        .setServerId(loadPos.getServerId())
                                        .setTimestamp(loadPos.getTimestamp())
                                        .setTimestampId(loadPos.getTimestampId())
                                        .setRowId(loadPos.getRowId())
                                        .setUId(loadPos.getUniqueId())
                                        .build();
                            }
                        }
                    } else { //fetch mode
                        // make cur position
                        curPos = EventEntry.RowMsgPos.newBuilder()
                                .setLogFile(entry.getHeader().getLogfileName())
                                .setLogPos(entry.getHeader().getExecuteTime())
                                .setServerId(entry.getHeader().getServerId())
                                .setTimestamp(entry.getHeader().getExecuteTime())
                                .setTimestampId(timestampId)
                                .setRowId(rowId)
                                .setUId(uniqueId)
                                .build();
                        //make row data message
                        EventEntry.RowMsg rowMsg = EventEntry.RowMsg.newBuilder()
                                .setHeader(entry.getHeader())
                                .setTableId(rowChange.getTableId())
                                .setEventType(rowChange.getEventType())
                                .setIsDdl(rowChange.getIsDdl())
                                .setSql(rowChange.getSql())
                                .setProps(0, EventEntry.Pair.newBuilder().setKey("ip").setValue(host))
                                .setProps(1, EventEntry.Pair.newBuilder().setKey("ft").setValue(String.valueOf(fetchTime)))
                                .setDdlSchemaName(rowChange.getDdlSchemaName())
                                .setRow(rowData)
                                .setLastPos(lastPos)
                                .setCurPos(curPos)
                                .build();
                        RowMsgTuple rowTuple = new RowMsgTuple(rowMsg);
                        tupleList.add(rowTuple);
                        //end op of loop
                        lastPos = curPos;// save the current position as the last position
                        //fetch mode 's row data just can be set uniqueId
                        uniqueId++;
                    }
                    rowId++;
                }
                //next event
            }
        }
        return tupleList;
    }

    /**
     * put the row message tuple to queue
     * @param tuple row message tuple
     * @throws Exception
     */
    @Override
    public void pipeTo(Tuple tuple) throws Exception {
        queue.put(tuple);
    }

    /**
     * pipe a list of tuple to queue
     * @param tuples a list of tuple
     * @throws Exception
     */
    @Override
    public void pipeTo(List<Tuple> tuples) throws Exception {
        for(Tuple tuple : tuples) {
            RowMsgTuple rowTuple = (RowMsgTuple) tuple;
            queue.put(rowTuple);
        }
    }

    /**
     * snap the end of row message tuple (Do not persist checkpoint)
     * @param tuple end of tuple
     * @throws Exception
     */
    @Override
    public void snapShot(Tuple tuple) throws Exception {
        snapTuple = tuple;
    }

    /**
     * snap shot the end of a list of message tuple
     * @param tuples a list of tuples
     * @throws Exception
     */
    @Override
    public void snapShot(List<Tuple> tuples) throws Exception {
        snapTuple = tuples.get(tuples.size() - 1);
    }

    /**
     * close the driver
     * @throws Exception
     */
    @Override
    public void close() throws Exception {
        tableConn.disconnect();
    }
}
