package com.github.hackerwin7.mysql.binlog.tracker.commons;

/**
 * Created by hp on 4/13/15.
 * the infrastructure of executor
 */
public interface TrackerInterface {
    //do something before run(),  that is only execute once
    public void prepare(String id) throws Exception;

    //do main process , that is run once or many times in loop
    public void run() throws Exception;

    //reload the job
    public void reload(String id) throws Exception;

    //close the job
    public void close(String id) throws Exception;

    //pause the job
    public void pause(String id) throws Exception;

    //exit the job
    public void exit() throws Exception;
}
