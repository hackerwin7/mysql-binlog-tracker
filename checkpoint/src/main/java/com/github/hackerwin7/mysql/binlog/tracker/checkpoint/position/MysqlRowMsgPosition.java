package com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.Position;

/**
 * row message position
 * Created by hp on 7/15/15.
 */
public class MysqlRowMsgPosition extends Position {

    /*data*/
    private String logFile = null;
    private long logPos = 0;
    private long bid = 0;
    private long iid = 0;
    private long uid = 0;

    /**
     * constructor for builder
     */
    private MysqlRowMsgPosition(MysqlRowMsgPositionBuilder builder) {
        logFile = builder.logFile;
        logPos = builder.logPos;
        bid = builder.bid;
        iid = builder.iid;
        uid = builder.uid;
    }

    /**
     * create builder
     * @return builder
     * @throws Exception
     */
    public static MysqlRowMsgPositionBuilder createBuilder() throws Exception {
        return new MysqlRowMsgPositionBuilder();
    }

    /*instance builder*/
    public static class MysqlRowMsgPositionBuilder {

        /*data*/
        private String logFile = null;
        private long logPos = 0;
        private long bid = 0;
        private long iid = 0;
        private long uid = 0;

        /**
         * constructor for create builder
         */
        private MysqlRowMsgPositionBuilder() {

        }

        /*setter*/
        public MysqlRowMsgPositionBuilder setLogFile(String _logFile) {
            logFile = _logFile;
            return this;
        }

        public MysqlRowMsgPositionBuilder setLogPos(long _logPos) {
            logPos = _logPos;
            return this;
        }

        public MysqlRowMsgPositionBuilder setBid(long batchId) {
            bid = batchId;
            return this;
        }

        public MysqlRowMsgPositionBuilder setiid(long inBatchId) {
            iid = inBatchId;
            return this;
        }

        public MysqlRowMsgPositionBuilder setuid(long uniqueId) {
            uid = uniqueId;
            return this;
        }

        public MysqlRowMsgPosition build() {
            return new MysqlRowMsgPosition(this);
        }

    }

    /**
     * parse checkpoint to position
     * @param cp
     * @return position
     * @throws Exception
     */
    @Override
    public Position toPosition(String cp) throws Exception {
        String[] ss = cp.split(":");
        String _logFile = ss[0];
        long _logPos = Long.valueOf(ss[1]);
        long _bid = Long.valueOf(ss[2]);
        long _iid = Long.valueOf(ss[3]);
        long _uid = Long.valueOf(ss[4]);
        return MysqlRowMsgPosition.createBuilder()
                .setLogFile(_logFile)
                .setLogPos(_logPos)
                .setBid(_bid)
                .setiid(_iid)
                .setuid(_uid)
                .build();
    }

    /**
     * parse position to checkpoint
     * @param _pos
     * @return
     * @throws Exception
     */
    @Override
    public String toCheckpoint(Position _pos) throws Exception {
        MysqlRowMsgPosition pos = (MysqlRowMsgPosition) _pos;
        StringBuilder sb = new StringBuilder();
        return sb.append(pos.getLogFile())
                .append(":")
                .append(pos.getLogPos())
                .append(":")
                .append(pos.getBid())
                .append(":")
                .append(pos.getIid())
                .append(":")
                .append(pos.getUid()).toString();
    }

    /**
     * compare two positions
     * @param _pos mysqlRowMsgPosition
     * @return 1,0,-1
     * @throws Exception
     */
    @Override
    public int compareTo(Position _pos) throws Exception {
        MysqlRowMsgPosition pos = (MysqlRowMsgPosition) _pos;
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
                return 0;
            }
        }
    }

    /*getter*/
    public long getUid() {
        return uid;
    }

    public String getLogFile() {
        return logFile;
    }

    public long getLogPos() {
        return logPos;
    }

    public long getBid() {
        return bid;
    }

    public long getIid() {
        return iid;
    }
}
