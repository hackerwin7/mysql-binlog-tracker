package com.github.hackerwin7.mysql.binlog.tracker.commons.exception;

/**
 * Created by hp on 4/21/15.
 */
public class ReloadException extends Exception {
    private static final long  serialVersionUID     =                           1230987657534245L;

    public ReloadException() {
        super();
    }

    public ReloadException(String msg) {
        super(msg);
    }

    public ReloadException(String msg, Throwable cause) {
        super(msg, cause);
    }

    public void throwEx() throws Exception {
        throw new Exception("reload the job completely.....");
    }

    public void throwTh() throws Throwable {
        throw new Throwable("reload the job completely......");
    }
}
