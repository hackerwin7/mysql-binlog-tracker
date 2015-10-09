package com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.mariadb;

import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogBuffer;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.FormatDescriptionLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.IgnorableLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.LogHeader;

/**
 * mariadb10的BINLOG_CHECKPOINT_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午2:22:04
 * @since 1.0.17
 */
public class BinlogCheckPointLogEvent extends IgnorableLogEvent {

    public BinlogCheckPointLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        // do nothing , just mariadb binlog checkpoint
    }

}
