package com.github.hackerwin7.mysql.binlog.tracker.tracker.common;

import org.apache.commons.lang.exception.NestableRuntimeException;

/**
 * Created by hp on 14-9-3.
 */
public class EventParseException extends NestableRuntimeException {

    private static final long serialVersionUID = -7288830284122672209L;

    public EventParseException(String errorCode){
        super(errorCode);
    }

    public EventParseException(String errorCode, Throwable cause){
        super(errorCode, cause);
    }

    public EventParseException(String errorCode, String errorDesc){
        super(errorCode + ":" + errorDesc);
    }

    public EventParseException(String errorCode, String errorDesc, Throwable cause){
        super(errorCode + ":" + errorDesc, cause);
    }

    public EventParseException(Throwable cause){
        super(cause);
    }

    public Throwable fillInStackTrace() {
        return this;
    }

}
