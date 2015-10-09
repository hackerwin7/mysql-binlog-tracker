package com.github.hackerwin7.mysql.binlog.tracker.checkpoint;

/**
 * Created by hp on 7/9/15.
 */
public abstract class Position {

    /*data*/
    protected Position last;
    protected Position next;

    /*setter*/
    public void setLast(Position _last) {
        last = _last;
    }

    public void setNext(Position _next) {
        next = _next;
    }

    /*getter*/
    public Position getLast() {
        return last;
    }

    public Position getNext() {
        return next;
    }

    public Position getCurrent() {
        return this;
    }

    /**
     * parse checkpoint to position
     * @param cp
     * @return position
     * @throws Exception
     */
    public abstract Position toPosition(String cp) throws Exception;

    /**
     * parse position to checkpoint
     * @param pos
     * @return checkpoint
     * @throws Exception
     */
    public abstract String toCheckpoint(Position pos) throws Exception;

    /**
     * position must be sortable
     * @param pos position
     * @return 1 (>), 0 (=), -1 (<)
     * @throws Exception
     */
    public abstract int compareTo(Position pos) throws Exception;
}
