package com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe;

/**
 * Created by hp on 7/24/15.
 */
public class CheckpointPipeConf extends FilePipeConf {

    /**
     * constructor
     * @throws Exception
     */
    public CheckpointPipeConf() throws Exception {

    }

    /*getter*/

    public String getCheckpointZk() {
        return checkpointZk;
    }

    public String getCheckpointZkRoot() {
        return checkpointZkRoot;
    }
}
