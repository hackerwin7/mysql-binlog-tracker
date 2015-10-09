package com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.mariadb;

import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogBuffer;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.IgnorableLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.LogHeader;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.FormatDescriptionLogEvent;

/**
 * mariadb的GTID_LIST_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午4:51:50
 * @since 1.0.17
 */
public class MariaGtidListLogEvent extends IgnorableLogEvent {

    public MariaGtidListLogEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);
        // do nothing , just ignore log event
    }

}
