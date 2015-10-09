package com.github.hackerwin7.mysql.binlog.tracker.tracker;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.Position;
import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.checkpointer.MysqlRowMsgPositionUtil4Zk;
import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position.MysqlRowMsgTimestampPosition;
import com.github.hackerwin7.mysql.binlog.tracker.commons.TrackerInterface;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.CheckpointPipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.FetchPipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.FilePipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.ParsePipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.PersistPipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.Pipe;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.fetch.FetchPipe;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.parse.ParsePipe;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent.PersistPipe;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent.checkpoint.pipe.PipeCheckpoint4Zk;
import org.apache.log4j.Logger;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * pipe mode for mysql tracker
 * Created by hp on 7/9/15.
 */
public class TrackerPipeHandler implements TrackerInterface {

    /*logger*/
    private Logger logger = Logger.getLogger(TrackerPipeHandler.class);

    /*config*/
    private FilePipeConf config = null;

    /*pipe*/
    private Pipe fpipe = null;
    private Pipe dpipe = null;
    private Pipe ppipe = null;

    /*checkpoint position*/
    private Position cp = null;

    /*checkpoint persist*/
    private PipeCheckpoint4Zk cpipe = null;

    /**
     * prepare for the job
     * @param jobId, task number
     * @throws Exception
     */
    @Override
    public void prepare(String jobId) throws Exception {

        //init
        initConfig();
        initCommon();
        initCheckpoint();

        //init pipe
        fpipe = new FetchPipe((FetchPipeConf) config, new ArrayBlockingQueue<LogEvent>(config.getQueuesize()), (MysqlRowMsgTimestampPosition) cp);
        dpipe = new ParsePipe((ParsePipeConf) config, new ArrayBlockingQueue<EventEntry.RowMsg>(config.getQueuesize()), fpipe.getRearBlockingQueue(), (MysqlRowMsgTimestampPosition) cp);
        ppipe = new PersistPipe((PersistPipeConf) config, dpipe.getRearBlockingQueue());

        //start pipes
        fpipe.start();
        dpipe.start();
        ppipe.starts();
    }

    /**
     * init the summary configuration by read file, url, service etc...
     * @throws Exception
     */
    public void initConfig() throws Exception {
        config = new FilePipeConf();
        config.load();
    }

    /**
     * init the common data, var , driver etc...
     * @throws Exception
     */
    public void initCommon() throws Exception {

    }

    /**
     * find the checkpoint by zk, HBase or other services
     * @throws Exception
     */
    public void initCheckpoint() throws Exception {
        CheckpointPipeConf cpConf = (CheckpointPipeConf) config;
        cp = MysqlRowMsgTimestampPosition.createBuilder().build();
        MysqlRowMsgPositionUtil4Zk cpUtil = new MysqlRowMsgPositionUtil4Zk(cpConf.getCheckpointZk());
        String checkpoint = cpUtil.read(cpConf.getCheckpointZkRoot() + "/" + cpConf.getJobId());
        cp = cp.toPosition(checkpoint);
    }

    /**
     * check the status
     */
    @Override
    public void run() throws Exception {

    }

    /**
     * pause the job
     * @param jobId
     * @throws Exception
     */
    @Override
    public void pause(String jobId) throws Exception {

    }

    /**
     * reload the job
     * @param jobId
     * @throws Exception
     */
    @Override
    public void reload(String jobId) throws Exception {
        /*no op*/
    }

    /**
     * close the job
     * @param jobId
     * @throws Exception
     */
    @Override
    public void close(String jobId) throws Exception {

    }

    /**
     * exit the program
     * @throws Exception
     */
    @Override
    public void exit() throws Exception {
        System.exit(1);
    }

}
