package com.github.hackerwin7.mysql.binlog.tracker.tracker.meta;

/**
 * Created by hp on 4/28/15.
 */
public class PositionOffset {

    private String                                  logfile                     = "";
    private long                                    offset                      = 0;
    private long                                    bid                         = 0;
    private long                                    iid                         = 0;
    private long                                    mid                         = 0;

    public PositionOffset() {

    }

    public PositionOffset(long mid, long iid, long bid, long offset, String logfile) {
        this.mid = mid;
        this.iid = iid;
        this.bid = bid;
        this.offset = offset;
        this.logfile = logfile;
    }

    public PositionOffset(PositionOffset _offset) {
        mid = _offset.getMid();
        iid = _offset.getIid();
        bid = _offset.getBid();
        offset = _offset.getOffset();
        logfile = _offset.getLogfile();
    }

    public String getPersistent() {
        return logfile + ":" + offset + ":" + bid + ":" + iid + ":" + mid;
    }

    public boolean isValid() {
        if(offset <= 0 && (bid + iid + mid) <= 0) {
            return false;
        } else {
            return true;
        }
    }

    public static boolean isAvailable(PositionOffset offset) {
        if(offset == null || !offset.isValid()) {
            return false;
        } else {
            return true;
        }
    }

    public long getMid() {
        return mid;
    }

    public long getIid() {
        return iid;
    }

    public long getBid() {
        return bid;
    }

    public long getOffset() {
        return offset;
    }

    public String getLogfile() {
        return logfile;
    }

    public void setLogfile(String logfile) {
        this.logfile = logfile;
    }

    public void setOffset(long offset) {
        this.offset = offset;
    }

    public void setBid(long bid) {
        this.bid = bid;
    }

    public void setIid(long iid) {
        this.iid = iid;
    }

    public void setMid(long mid) {
        this.mid = mid;
    }
}
