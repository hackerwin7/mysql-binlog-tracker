package com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent.checkpoint;

import com.github.hackerwin7.mysql.binlog.tracker.commons.AbstractDriver;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.PositionOffset;
import org.apache.log4j.Logger;

/**
 * Created by hp on 4/28/15.
 */
public class PositionPersistent extends AbstractDriver {
    private Logger                                  logger                      = Logger.getLogger(PositionPersistent.class);

    protected PositionOffset                        position                    = null;

    /**
     * persistent the position
     * @throws Exception
     */
    protected void persistent() throws Exception {

    }

    /**
     *
     * @param pos
     */
    public void setPosition(PositionOffset pos) {
        position = pos;
    }

    public PositionOffset getPosition() {
        return position;
    }
}
