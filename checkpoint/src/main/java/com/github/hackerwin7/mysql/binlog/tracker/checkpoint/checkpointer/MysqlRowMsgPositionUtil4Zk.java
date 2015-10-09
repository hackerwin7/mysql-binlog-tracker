package com.github.hackerwin7.mysql.binlog.tracker.checkpoint.checkpointer;

/**
 * Created by hp on 7/15/15.
 */
public class MysqlRowMsgPositionUtil4Zk extends MysqlEventPositionUtil4Zk {

    /**
     * constructor
     * @param zkServer
     * @throws Exception
     */
    public MysqlRowMsgPositionUtil4Zk(String zkServer) throws Exception {
        super(zkServer);
    }

}
