package com.github.hackerwin7.mysql.binlog.tracker.tracker.persistent.checkpoint.pipe;

import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.ICheckpoint;
import com.github.hackerwin7.mysql.binlog.tracker.checkpoint.position.MysqlRowMsgTimestampPosition;
import com.github.hackerwin7.mysql.binlog.tracker.config.impl.pipe.CheckpointPipeConf;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.Pipe;
import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.meta.tuple.RowMsgTuple;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.conf.ZookeeperConf;
import com.github.hackerwin7.mysql.binlog.tracker.utils.zookeeper.driver.ZkExecutor;

import java.util.ArrayList;
import java.util.List;

/**
 * per minute persist position
 * Created by hp on 7/20/15.
 */
public abstract class PipeCheckpoint4Zk extends Pipe implements ICheckpoint {

    /*data*/
    private String zks = null;
    private String zkr = null;
    private String zkp = null;

    /*source, source of checkpoint, it can be get constantly and it will be changed constantly, not invariable
    * pipe source 's snap tuple can be a checkpoint source*/
    private Pipe source = null;

    /*driver*/
    private ZkExecutor zk = null;

    /**
     * constructor
     * @param conf , zk connection string
     * @param _source, the source of checkpoint from that
     * @throws Exception
     */
    public PipeCheckpoint4Zk(CheckpointPipeConf conf, Pipe _source) throws Exception {
        super(null);
        zks = conf.getCheckpointZk();
        zkr = conf.getCheckpointZkRoot();
        zkp = zkr + "/" + conf.getJobId();
        source = _source;
    }

    /**
     * init the zk driver
     * @throws Exception
     */
    @Override
    public void init() throws Exception {
        zk = new ZkExecutor(new ZookeeperConf(zks));
        zk.connect();
        zk.create(zkr, null);
    }

    /**
     * read the checkpoint
     * @param jobId, job number id
     * @return checkpoint
     * @throws Exception
     */
    @Override
    public String read(String jobId) throws Exception {
        return zk.get(zkr + "/" + jobId);
    }

    /**
     * read itself
     * @return checkpoint
     * @throws Exception
     */
    public String read() throws Exception {
        return zk.get(zkp);
    }

    /**
     * write the checkpoint
     * @param jobId, zk path
     * @param value, zk value
     * @throws Exception
     */
    @Override
    public void write(String jobId, String value) throws Exception {
        zk.set(zkr + "/" + jobId, value);
    }

    /**
     * write itself
     * @throws Exception
     */
    public void write(String value) throws Exception {
        write(zkp, value);
    }

    /**
     * prepare for pipe
     * @throws Exception
     */
    @Override
    public void prePipe() throws Exception {
        /*no op*/
    }

    /**
     * pipe from the source
     * @return snap tuple of source
     * @throws Exception
     */
    @Override
    public Tuple pipeFrom() throws Exception {
        return source.getSnapTuple();
    }

    /**
     * pipe a batch of tuples
     * @return a list of tuple
     * @throws Exception
     */
    @Deprecated
    @Override
    public List<Tuple> pipeBatchFrom() throws Exception {
        List<Tuple> tuples = new ArrayList<Tuple>();
        tuples.add(source.getSnapTuple());
        return tuples;
    }

    /**
     * no operation
     * @param tuple, snap tuple
     * @return snap tuple
     * @throws Exception
     */
    @Override
    public Tuple dealTuple(Tuple tuple) throws Exception {
        /*no op*/
        return tuple;
    }

    /**
     * a list of tuple
     * @param tuples, snap tuple
     * @return snap tuple
     * @throws Exception
     */
    @Override
    public List<Tuple> dealTuple(List<Tuple> tuples) throws Exception {
        return tuples;
    }

    /**
     * zk persist for
     * @param tuple, snap tuple
     * @throws Exception
     */
    @Override
    public void pipeTo(Tuple tuple) throws Exception {
        //get current position which inner the row message
        RowMsgTuple rowTuple = (RowMsgTuple) tuple;
        EventEntry.RowMsg rowMsg = rowTuple.getRowMsg();
        EventEntry.RowMsgPos curPos = rowMsg.getCurPos();
        MysqlRowMsgTimestampPosition pos = MysqlRowMsgTimestampPosition.createBuilder().build();
        pos.cloneFrom(curPos);
        write(pos.toCheckpoint());
    }

    /**
     * persist end of tuple inner the tuples
     * @param tuples, a list of tuple
     * @throws Exception
     */
    @Override
    public void pipeTo(List<Tuple> tuples) throws Exception {
        pipeTo(tuples.get(tuples.size() - 1));
    }

    /**
     * snap shot the end of tuple
     * @param tuple,  snap tuple
     * @throws Exception
     */
    @Override
    public void snapShot(Tuple tuple) throws Exception {
        snapTuple = tuple;
    }

    /**
     * snap shot the end of the tuple
     * @param tuples, a list of tuple
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
        zk.disconnect();
        zk.close();
    }



}
