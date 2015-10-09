package com.github.hackerwin7.mysql.binlog.tracker.pipe;


import com.github.hackerwin7.mysql.binlog.tracker.pipe.data.Tuple;

import java.util.List;

/**
 * Created by hp on 7/3/15.
 */
public interface IPipe {

    /**
     * init the pipe driver
     * @throws Exception
     */
    public void init() throws Exception;

    /**
     * previously piping
     * @throws Exception
     */
    public void prePipe() throws Exception;

    /**
     * pipe from the source
     * @return tuple
     * @throws Exception
     */
    public Tuple pipeFrom() throws Exception;

    /**
     * pipe a batch of tuples from source
     * @return tuples
     * @throws Exception
     */
    public List<Tuple> pipeBatchFrom() throws Exception;

    /**
     * take some operation to tuple
     * @param tuple
     * @return tuple
     * @throws Exception
     */
    public Tuple dealTuple(Tuple tuple) throws Exception;

    /**
     * take some operation to tuples
     * @param tuples
     * @return tuples
     * @throws Exception
     */
    public List<Tuple> dealTuple(List<Tuple> tuples) throws Exception;

    /**
     * put tuple into target
     * @param tuple
     * @throws Exception
     */
    public void pipeTo(Tuple tuple) throws Exception;

    /**
     * put tuples into target
     * @param tuples
     * @throws Exception
     */
    public void pipeTo(List<Tuple> tuples) throws Exception;

    /**
     * snapshot the end of tuple
     * @param tuple
     * @throws Exception
     */
    public void snapShot(Tuple tuple) throws Exception;

    /**
     * snapshot the end of tuple
     * @param tuple
     * @throws Exception
     */
    public void snapShot(List<Tuple> tuple) throws Exception;

    /**
     * stop the piping and close the driver connection
     * @throws Exception
     */
    public void close() throws Exception;
}
