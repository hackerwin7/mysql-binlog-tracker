package com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple;

import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;

/**
 * Created by hp on 7/17/15.
 */
public class RowMsgTuple extends Tuple {

    /*data*/
    private EventEntry.RowMsg rowMsg = null;

    /**
     * constructor
     * @param _rowMsg protobuf row message
     * @throws Exception
     */
    public RowMsgTuple(EventEntry.RowMsg _rowMsg) throws Exception {
        rowMsg = _rowMsg;
    }

    /**
     * parse tuple to bytes
     * @return bytes
     * @throws Exception
     */
    @Override
    public byte[] toBytes() throws Exception {
        return rowMsg.toByteArray();
    }

    /**
     * parse bytes to tuple
     * @param bytes byte array
     * @return row message tuple
     * @throws Exception
     */
    @Override
    public Tuple parseFrom(byte[] bytes) throws Exception {
        EventEntry.RowMsg msg = EventEntry.RowMsg.parseFrom(bytes);
        return new RowMsgTuple(msg);
    }

    /**
     * row message is null or not
     * @return boolean
     * @throws Exception
     */
    @Override
    public boolean isBlank() throws Exception {
        if(rowMsg == null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * compare to the offset
     * @param tuple, compared tuple
     * @return boolean
     * @throws Exception
     */
    public boolean isOffsetEqual(RowMsgTuple tuple) throws Exception {
        EventEntry.RowMsg compareMsg = tuple.getRowMsg();
        String logFile1 = rowMsg.getHeader().getLogfileName();
        long logPos1 = rowMsg.getHeader().getLogfileOffset();
        String logFile2 = compareMsg.getHeader().getLogfileName();
        long logPos2 = compareMsg.getHeader().getLogfileOffset();
        if(logFile1.equals(logFile2) && logPos1 == logPos2) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * getter
     * @return row message
     */
    public EventEntry.RowMsg getRowMsg() {
        return rowMsg;
    }
}
