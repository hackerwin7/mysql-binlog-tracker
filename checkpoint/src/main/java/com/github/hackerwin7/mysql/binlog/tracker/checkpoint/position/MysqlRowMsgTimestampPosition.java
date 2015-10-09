package com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.Position;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;

/**
 *  timestamp mysql row message
 * Created by hp on 7/21/15.
 */
public class MysqlRowMsgTimestampPosition extends Position {

    /*data*/
    private String logFile = null;
    private long logPos = 0;
    private long serverId = 0;
    private long timestamp = 0;
    private long timestampId = 0;
    private long rowId = 0;
    private long uniqueId = 0;

    /**
     * constructor by using builder
     * @param builder builder mode
     */
    private MysqlRowMsgTimestampPosition(MysqlRowMsgTimestampPositionBuilder builder) {
        logFile = builder.logFile;
        logPos = builder.logPos;
        serverId = builder.serverId;
        timestamp = builder.timestamp;
        timestampId = builder.timestampId;
        rowId = builder.rowId;
        uniqueId = builder.uniqueId;
    }

    /**
     * create builder
     * @return builder
     * @throws Exception
     */
    public static MysqlRowMsgTimestampPositionBuilder createBuilder() throws Exception {
        return new MysqlRowMsgTimestampPositionBuilder();
    }

    /**
     * builder mode
     */
    public static class MysqlRowMsgTimestampPositionBuilder {
        /*data*/
        private String logFile = null;
        private long logPos = 0;
        private long serverId = 0;
        private long timestamp = 0;
        private long timestampId = 0;
        private long rowId = 0;
        private long uniqueId = 0;

        private MysqlRowMsgTimestampPositionBuilder() {

        }

        /*setter*/
        public MysqlRowMsgTimestampPositionBuilder setLogFile(String _logFile) {
            logFile = _logFile;
            return this;
        }

        public MysqlRowMsgTimestampPositionBuilder setLogPos(long _logPos) {
            logPos = _logPos;
            return this;
        }

        public MysqlRowMsgTimestampPositionBuilder setServerId(long _serverId) {
            serverId = _serverId;
            return this;
        }

        public MysqlRowMsgTimestampPositionBuilder setTimestamp(long _timestamp) {
            timestamp = _timestamp;
            return this;
        }

        public MysqlRowMsgTimestampPositionBuilder setTimestampId(long _timestampId) {
            timestampId = _timestampId;
            return this;
        }

        public MysqlRowMsgTimestampPositionBuilder setRowId(long _rowId) {
            rowId = _rowId;
            return this;
        }

        public MysqlRowMsgTimestampPositionBuilder setUniqueId(long _uniqueId) {
            uniqueId = _uniqueId;
            return this;
        }

        public MysqlRowMsgTimestampPosition build() {
            return new MysqlRowMsgTimestampPosition(this);
        }
    }

    /**
     * parse checkpoint to position
     * @param cp checkpoint
     * @return mysql row message timestamp position
     * @throws Exception
     */
    @Override
    public Position toPosition(String cp) throws Exception {
        String[] ss = cp.split(":");
        return MysqlRowMsgTimestampPosition.createBuilder()
                .setLogFile(ss[0])
                .setLogPos(Long.valueOf(ss[1]))
                .setServerId(Long.valueOf(ss[2]))
                .setTimestamp(Long.valueOf(ss[3]))
                .setTimestampId(Long.valueOf(ss[4]))
                .setRowId(Long.valueOf(ss[5]))
                .setUniqueId(Long.valueOf(ss[6]))
                .build();
    }

    /**
     * parse position to checkpoint
     * @param _pos position
     * @return string of checkpoint
     * @throws Exception
     */
    @Override
    public String toCheckpoint(Position _pos) throws Exception {
        MysqlRowMsgTimestampPosition pos = (MysqlRowMsgTimestampPosition) _pos;
        StringBuilder sb = new StringBuilder();
        return sb
                .append(pos.getLogFile())
                .append(":")
                .append(pos.getLogPos())
                .append(":")
                .append(pos.getServerId())
                .append(":")
                .append(pos.getTimestamp())
                .append(":")
                .append(pos.getTimestampId())
                .append(":")
                .append(pos.getRowId())
                .append(":")
                .append(pos.getUniqueId())
                .toString();
    }

    /**
     * parse itself to checkpoint
     * @return checkpoint string
     * @throws Exception
     */
    public String toCheckpoint() throws Exception {
        return toCheckpoint(this);
    }

    /**
     * compare two position
     * @param _pos position
     * @return 1, 0, -1
     * @throws Exception
     */
    @Override
    public int compareTo(Position _pos) throws Exception {
        MysqlRowMsgTimestampPosition pos = (MysqlRowMsgTimestampPosition) _pos;
        int cmp = logFile.compareTo(pos.getLogFile());
        if(cmp > 0) {
            return 1;
        } else if(cmp < 0) {
            return -1;
        } else {
            if(logPos > pos.getLogPos()) {
                return 1;
            } else if(logPos < pos.getLogPos()) {
                return -1;
            } else {
                if(timestamp > pos.getTimestamp()) {
                    return 1;
                } else if(timestamp < pos.getTimestamp()) {
                    return -1;
                } else {
                    if(timestampId > pos.getTimestampId()) {
                        return 1;
                    } else if(timestampId < pos.getTimestampId()) {
                        return -1;
                    } else {
                        if(rowId > pos.getRowId()) {
                            return 1;
                        } else if(rowId < pos.getRowId()) {
                            return -1;
                        } else {
                            return 0;
                        }
                    }
                }
            }
        }
    }

    /**
     * how many step between this and _pos,
     * @param _pos mysqlRowMsgTimestampPosition
     * @return the count of step, can be 0 or > 0  or < 0
     * @throws Exception
     */
    public int distanceTo(Position _pos) throws Exception {
        return compareTo(_pos);
    }


    /**
     * clone data from protobuf data
     * @throws Exception
     */
    public void cloneFrom(EventEntry.RowMsgPos pos) throws Exception {
        logFile = pos.getLogFile();
        logPos = pos.getLogPos();
        serverId = pos.getServerId();
        timestamp = pos.getTimestamp();
        timestampId = pos.getTimestampId();
        rowId = pos.getRowId();
        uniqueId = pos.getUId();
    }

    /*getter*/
    public String getLogFile() {
        return logFile;
    }

    public long getLogPos() {
        return logPos;
    }

    public long getServerId() {
        return serverId;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public long getTimestampId() {
        return timestampId;
    }

    public long getRowId() {
        return rowId;
    }

    public long getUniqueId() {
        return uniqueId;
    }

}
