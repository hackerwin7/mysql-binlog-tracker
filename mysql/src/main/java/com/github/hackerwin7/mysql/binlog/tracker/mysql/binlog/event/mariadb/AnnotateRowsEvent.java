package com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.mariadb;

import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogBuffer;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.IgnorableLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.LogHeader;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.FormatDescriptionLogEvent;

/**
 * mariadb的ANNOTATE_ROWS_EVENT类型
 * 
 * @author jianghang 2014-1-20 下午2:20:35
 * @since 1.0.17
 */
public class AnnotateRowsEvent extends IgnorableLogEvent {

    private String rowsQuery;

    public AnnotateRowsEvent(LogHeader header, LogBuffer buffer, FormatDescriptionLogEvent descriptionEvent){
        super(header, buffer, descriptionEvent);

        final int commonHeaderLen = descriptionEvent.getCommonHeaderLen();
        final int postHeaderLen = descriptionEvent.getPostHeaderLen()[header.getType() - 1];

        int offset = commonHeaderLen + postHeaderLen;
        int len = buffer.limit() - offset;
        rowsQuery = buffer.getFullString(offset, len, LogBuffer.ISO_8859_1);
    }

    public String getRowsQuery() {
        return rowsQuery;
    }

}
