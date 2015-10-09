package com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event;

import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogBuffer;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;

/**
 * Delete_file_log_event.
 * 
 * @author <a href="mailto:changyuan.lh@taobao.com">Changyuan.lh</a>
 * @version 1.0
 */
public final class DeleteFileLogEvent extends LogEvent
{
    private final long      fileId;

    /* DF = "Delete File" */
    public static final int DF_FILE_ID_OFFSET = 0;

    public DeleteFileLogEvent(LogHeader header, LogBuffer buffer,
            FormatDescriptionLogEvent descriptionEvent)
    {
        super(header);

        final int commonHeaderLen = descriptionEvent.commonHeaderLen;
        buffer.position(commonHeaderLen + DF_FILE_ID_OFFSET);
        fileId = buffer.getUint32(); //  DF_FILE_ID_OFFSET
    }

    public final long getFileId()
    {
        return fileId;
    }
}
