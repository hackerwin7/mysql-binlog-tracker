package com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.Position;

/**
 * event checkpoint
 * Created by hp on 7/9/15.
 */
public class MysqlEventPosition extends Position {

    /*data*/
    private String logFile = null;
    private long logPos = 0;

    /*driver*/

    /**
     * constructor
     * @throws Exception
     */
    public MysqlEventPosition() throws Exception {

    }

    /**
     * constructor
     * @param _logFile
     * @param _logPos
     * @throws Exception
     */
    public MysqlEventPosition(String _logFile, long _logPos) throws Exception {
        logFile = _logFile;
        logPos = _logPos;
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
        return new MysqlEventPosition(_logFile, _logPos);
    }

    /**
     * parse position to checkpoint
     * @param pos
     * @return checkpoint
     * @throws Exception
     */
    @Override
    public String toCheckpoint(Position _pos) throws Exception {
        MysqlEventPosition pos = (MysqlEventPosition) _pos;
        StringBuilder sb = new StringBuilder();
        sb.append(pos.getLogFile());
        sb.append(":");
        sb.append(pos.getLogPos());
        return sb.toString();
    }

    /*setter*/
    public void setLogPos(long logPos) {
        this.logPos = logPos;
    }

    public void setLogFile(String logFile) {
        this.logFile = logFile;
    }

    /*getter*/
    public String getLogFile() {
        return logFile;
    }

    public long getLogPos() {
        return logPos;
    }

    /**
     * compare two positions
     * @param _pos position
     * @return -1, 0, 1
     * @throws Exception
     */
    @Override
    public int compareTo(Position _pos) throws Exception {
        MysqlEventPosition pos = (MysqlEventPosition) _pos;
        int comp1 = logFile.compareTo(pos.getLogFile());
        if(comp1 > 0) {
            return 1;
        } else if( comp1 < 0) {
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
}
