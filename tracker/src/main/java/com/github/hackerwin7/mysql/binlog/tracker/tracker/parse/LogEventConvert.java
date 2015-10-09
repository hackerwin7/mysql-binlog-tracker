package com.github.hackerwin7.mysql.binlog.tracker.tracker.parse;

import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.EventParseException;
import com.google.protobuf.ByteString;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.LogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.DeleteRowsLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.IntvarLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.LogHeader;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.QueryLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.RandLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.RotateLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.RowsLogBuffer;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.RowsLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.RowsQueryLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.TableMapLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.UnknownLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.UpdateRowsLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.UserVarLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.WriteRowsLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.XidLogEvent;
import com.github.hackerwin7.mysql.binlog.tracker.mysql.binlog.event.mariadb.AnnotateRowsEvent;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.github.hackerwin7.mysql.binlog.tracker.protocol.protobuf.EventEntry;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.AviaterRegexFilter;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.TableMeta;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.TableMeta.FieldMeta;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.common.TableMetaCache;
import com.github.hackerwin7.mysql.binlog.tracker.tracker.parse.SimpleDdlParser.DdlResult;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.Charset;
import java.sql.Types;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by hp on 14-9-3.
 */
public class LogEventConvert {

    public static final String          ISO_8859_1          = "ISO-8859-1";
    public static final String          UTF_8               = "UTF-8";
    public static final int             TINYINT_MAX_VALUE   = 256;
    public static final int             SMALLINT_MAX_VALUE  = 65536;
    public static final int             MEDIUMINT_MAX_VALUE = 16777216;
    public static final long            INTEGER_MAX_VALUE   = 4294967296L;
    public static final BigInteger BIGINT_MAX_VALUE    = new BigInteger("18446744073709551616");
    public static final int             version             = 1;
    public static final String          BEGIN               = "BEGIN";
    public static final String          COMMIT              = "COMMIT";
    public static final Logger          logger              = LoggerFactory.getLogger(LogEventConvert.class);

    private volatile AviaterRegexFilter nameFilter;                                                          // 运行时引用可能会有变化，比如规则发生变化时
    private TableMetaCache tableMetaCache;

    public String getBinlogFileName() {
        return binlogFileName;
    }

    private String                      binlogFileName      = "mysql-bin.000001";
    private Charset charset             = Charset.defaultCharset();
    private boolean                     filterQueryDcl      = false;
    private boolean                     filterQueryDml      = false;
    private boolean                     filterQueryDdl      = false;

    //additional info by hackerwin7
    private Map<String, String> filterMap = new HashMap<String, String>();

    private Map<String, String> senseMap = new HashMap<String, String>();

    public void addfilterMap(String dbtb, String tb) {
        filterMap.put(dbtb, tb);
    }

    public void setFilterMap(Map<String, String> map) {
        filterMap.clear();
        filterMap.putAll(map);
    }

    public void addSenseMap(String dbtb, String field) {
        senseMap.put(dbtb, field);
    }

    public void setSenseMap(Map<String, String> map) {
        senseMap.clear();
        senseMap.putAll(map);
    }

    public EventEntry.Entry parse(LogEvent logEvent) throws EventParseException {
        if (logEvent == null || logEvent instanceof UnknownLogEvent) {
            return null;
        }



        int eventType = logEvent.getHeader().getType();
        switch (eventType) {
            case LogEvent.ROTATE_EVENT:
                logger.info("EVENT : rotate");
                binlogFileName = ((RotateLogEvent) logEvent).getFilename();
                break;
            case LogEvent.QUERY_EVENT:
                logger.info("EVENT : query");
                return parseQueryEvent((QueryLogEvent) logEvent);
            case LogEvent.XID_EVENT:
                logger.info("EVENT : xid");
                return parseXidEvent((XidLogEvent) logEvent);
            case LogEvent.TABLE_MAP_EVENT:
                logger.info("EVENT : table_map");
                break;
            case LogEvent.WRITE_ROWS_EVENT_V1:
            case LogEvent.WRITE_ROWS_EVENT:
                logger.info("EVENT : write_rows");
                return parseRowsEvent((WriteRowsLogEvent) logEvent);
            case LogEvent.UPDATE_ROWS_EVENT_V1:
            case LogEvent.UPDATE_ROWS_EVENT:
                logger.info("EVENT : update_rows");
                return parseRowsEvent((UpdateRowsLogEvent) logEvent);
            case LogEvent.DELETE_ROWS_EVENT_V1:
            case LogEvent.DELETE_ROWS_EVENT:
                logger.info("EVENT : delete_rows");
                return parseRowsEvent((DeleteRowsLogEvent) logEvent);
            case LogEvent.ROWS_QUERY_LOG_EVENT:
                logger.info("EVENT : rows_query");
                return parseRowsQueryEvent((RowsQueryLogEvent) logEvent);
            case LogEvent.ANNOTATE_ROWS_EVENT:
                logger.info("EVENT : annotate_rows");
                return parseAnnotateRowsEvent((AnnotateRowsEvent) logEvent);
            case LogEvent.USER_VAR_EVENT:
                logger.info("EVENT : user_var");
                return parseUserVarLogEvent((UserVarLogEvent) logEvent);
            case LogEvent.INTVAR_EVENT:
                logger.info("EVENT : intvar");
                return parseIntrvarLogEvent((IntvarLogEvent) logEvent);
            case LogEvent.RAND_EVENT:
                logger.info("EVENT : rand");
                return parseRandLogEvent((RandLogEvent) logEvent);
            default:
                break;
        }

        return null;
    }

    public void reset() {
        // do nothing
        binlogFileName = "mysql-bin.000001";
        if (tableMetaCache != null) {
            tableMetaCache.clearTableMeta();
        }
    }

    private EventEntry.Entry parseQueryEvent(QueryLogEvent event) {
        String queryString = event.getQuery();
        if (StringUtils.endsWithIgnoreCase(queryString, BEGIN)) {
            EventEntry.TransactionBegin transactionBegin = createTransactionBegin(event.getSessionId());
            EventEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, EventEntry.EntryType.TRANSACTIONBEGIN, transactionBegin.toByteString());
        } else if (StringUtils.endsWithIgnoreCase(queryString, COMMIT)) {
            EventEntry.TransactionEnd transactionEnd = createTransactionEnd(0L); // MyISAM可能不会有xid事件
            EventEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
            return createEntry(header, EventEntry.EntryType.TRANSACTIONEND, transactionEnd.toByteString());
        } else {
            // DDL语句处理
            DdlResult result = SimpleDdlParser.parse(queryString, event.getDbName());

            String schemaName = event.getDbName();
            if (StringUtils.isNotEmpty(result.getSchemaName())) {
                schemaName = result.getSchemaName();
            }

            String tableName = result.getTableName();
            //additional filter
            EventEntry.EventType type = EventEntry.EventType.QUERY;
            // fixed issue https://github.com/alibaba/canal/issues/58
            if (result.getType() == EventEntry.EventType.ALTER || result.getType() == EventEntry.EventType.ERASE
                    || result.getType() == EventEntry.EventType.CREATE || result.getType() == EventEntry.EventType.TRUNCATE
                    || result.getType() == EventEntry.EventType.RENAME || result.getType() == EventEntry.EventType.CINDEX
                    || result.getType() == EventEntry.EventType.DINDEX) { // 针对DDL类型

                if (filterQueryDdl) {
                    return null;
                }

                type = result.getType();
                if (StringUtils.isEmpty(tableName)
                        || (result.getType() == EventEntry.EventType.RENAME && StringUtils.isEmpty(result.getOriTableName()))) {
                    // 如果解析不出tableName,记录一下日志，方便bugfix，目前直接抛出异常，中断解析
                    throw new EventParseException("SimpleDdlParser process query failed. pls submit issue with this queryString: "
                            + queryString + " , and DdlResult: " + result.toString());
                    // return null;
                } else {
                    // check name filter
                    if (nameFilter != null && !nameFilter.filter(schemaName + "." + tableName)) {
                        if (result.getType() == EventEntry.EventType.RENAME) {
                            // rename校验只要源和目标满足一个就进行操作
                            if (nameFilter != null
                                    && !nameFilter.filter(result.getOriSchemaName() + "." + result.getOriTableName())) {
                                return null;
                            }
                        } else {
                            // 其他情况返回null
                            return null;
                        }
                    }
                }
            } else if (result.getType() == EventEntry.EventType.INSERT || result.getType() == EventEntry.EventType.UPDATE
                    || result.getType() == EventEntry.EventType.DELETE) {
                // 对外返回，保证兼容，还是返回QUERY类型，这里暂不解析tableName，所以无法支持过滤
                if (filterQueryDml) {
                    return null;
                }
            } else if (filterQueryDcl) {
                return null;
            }

            // 更新下table meta cache
            if (tableMetaCache != null
                    && (result.getType() == EventEntry.EventType.ALTER || result.getType() == EventEntry.EventType.ERASE || result.getType() == EventEntry.EventType.RENAME)) {
                if (StringUtils.isNotEmpty(tableName)) {
                    // 如果解析到了正确的表信息，则根据全名进行清除
                    tableMetaCache.clearTableMeta(schemaName, tableName);
                } else {
                    // 如果无法解析正确的表信息，则根据schema进行清除
                    tableMetaCache.clearTableMetaWithSchemaName(schemaName);
                }
            }

            EventEntry.Header header = createHeader(binlogFileName, event.getHeader(), schemaName, tableName, type);
            EventEntry.RowChange.Builder rowChangeBuider = EventEntry.RowChange.newBuilder();
            if (result.getType() != EventEntry.EventType.QUERY) {
                rowChangeBuider.setIsDdl(true);
            }
            rowChangeBuider.setSql(queryString);
            if (StringUtils.isNotEmpty(event.getDbName())) {// 可能为空
                rowChangeBuider.setDdlSchemaName(event.getDbName());
            }
            rowChangeBuider.setEventType(result.getType());
            return createEntry(header, EventEntry.EntryType.ROWDATA, rowChangeBuider.build().toByteString());
        }
    }

    private EventEntry.Entry parseRowsQueryEvent(RowsQueryLogEvent event) {
        if (filterQueryDml) {
            return null;
        }
        // mysql5.6支持，需要设置binlog-rows-query-log-events=1，可详细打印原始DML语句
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset.name());
            return buildQueryEntry(queryString, event.getHeader());
        } catch (UnsupportedEncodingException e) {
            throw new EventParseException(e);
        }
    }

    private EventEntry.Entry parseAnnotateRowsEvent(AnnotateRowsEvent event) {
        if (filterQueryDml) {
            return null;
        }
        // mariaDb支持，需要设置binlog_annotate_row_events=true，可详细打印原始DML语句
        String queryString = null;
        try {
            queryString = new String(event.getRowsQuery().getBytes(ISO_8859_1), charset.name());
            return buildQueryEntry(queryString, event.getHeader());
        } catch (UnsupportedEncodingException e) {
            throw new EventParseException(e);
        }
    }

    private EventEntry.Entry parseUserVarLogEvent(UserVarLogEvent event) {
        if (filterQueryDml) {
            return null;
        }

        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private EventEntry.Entry parseIntrvarLogEvent(IntvarLogEvent event) {
        if (filterQueryDml) {
            return null;
        }

        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private EventEntry.Entry parseRandLogEvent(RandLogEvent event) {
        if (filterQueryDml) {
            return null;
        }

        return buildQueryEntry(event.getQuery(), event.getHeader());
    }

    private EventEntry.Entry parseXidEvent(XidLogEvent event) {
        EventEntry.TransactionEnd transactionEnd = createTransactionEnd(event.getXid());
        EventEntry.Header header = createHeader(binlogFileName, event.getHeader(), "", "", null);
        return createEntry(header, EventEntry.EntryType.TRANSACTIONEND, transactionEnd.toByteString());
    }

    private EventEntry.Entry parseRowsEvent(RowsLogEvent event) {
        try {
            TableMapLogEvent table = event.getTable();
            if (table == null) {
                // tableId对应的记录不存在
                throw new EventParseException("not found tableId:" + event.getTableId());
            }

            String fullname = table.getDbName() + "." + table.getTableName();
            // check name filter
            if (nameFilter != null && !nameFilter.filter(fullname)) {
                return null;
            }
            // check my filter
            if(filterMap.size() > 0 && !filterMap.containsKey(fullname)) {
                return null;
            }
            EventEntry.EventType eventType = null;
            int type = event.getHeader().getType();
            if (LogEvent.WRITE_ROWS_EVENT_V1 == type || LogEvent.WRITE_ROWS_EVENT == type) {
                eventType = EventEntry.EventType.INSERT;
            } else if (LogEvent.UPDATE_ROWS_EVENT_V1 == type || LogEvent.UPDATE_ROWS_EVENT == type) {
                eventType = EventEntry.EventType.UPDATE;
            } else if (LogEvent.DELETE_ROWS_EVENT_V1 == type || LogEvent.DELETE_ROWS_EVENT == type) {
                eventType = EventEntry.EventType.DELETE;
            } else {
                throw new EventParseException("unsupport event type :" + event.getHeader().getType());
            }

            EventEntry.Header header = createHeader(binlogFileName,
                    event.getHeader(),
                    table.getDbName(),
                    table.getTableName(),
                    eventType);
            EventEntry.RowChange.Builder rowChangeBuider = EventEntry.RowChange.newBuilder();
            rowChangeBuider.setTableId(event.getTableId());
            rowChangeBuider.setIsDdl(false);

            rowChangeBuider.setEventType(eventType);
            RowsLogBuffer buffer = event.getRowsBuf(charset.name());
            BitSet columns = event.getColumns();
            BitSet changeColumns = event.getColumns();
            TableMeta tableMeta = null;
            if (tableMetaCache != null) {// 入错存在table meta cache
                tableMeta = tableMetaCache.getTableMeta(table.getDbName(), table.getTableName());
                if (tableMeta == null) {
                    throw new EventParseException("not found [" + fullname + "] in db , pls check!");
                }
            }

            while (buffer.nextOneRow(columns)) {
                // 处理row记录
                EventEntry.RowData.Builder rowDataBuilder = EventEntry.RowData.newBuilder();
                if (EventEntry.EventType.INSERT == eventType) {
                    // insert的记录放在before字段中
                    parseOneRow(rowDataBuilder, event, buffer, columns, true, tableMeta);
                } else if (EventEntry.EventType.DELETE == eventType) {
                    // delete的记录放在before字段中
                    parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta);
                } else {
                    // update需要处理before/after
                    parseOneRow(rowDataBuilder, event, buffer, columns, false, tableMeta);
                    if (!buffer.nextOneRow(changeColumns)) {
                        rowChangeBuider.addRowDatas(rowDataBuilder.build());
                        break;
                    }
                    parseOneRow(rowDataBuilder, event, buffer, event.getChangeColumns(), true, tableMeta);
                }

                rowChangeBuider.addRowDatas(rowDataBuilder.build());
            }
            return createEntry(header, EventEntry.EntryType.ROWDATA, rowChangeBuider.build().toByteString());
        } catch (Exception e) {
            throw new EventParseException("parse row data failed.", e);
        }
    }

    private void parseOneRow(EventEntry.RowData.Builder rowDataBuilder, RowsLogEvent event, RowsLogBuffer buffer, BitSet cols,
                             boolean isAfter, TableMeta tableMeta) throws UnsupportedEncodingException {
        final int columnCnt = event.getTable().getColumnCnt();
        final TableMapLogEvent.ColumnInfo[] columnInfo = event.getTable().getColumnInfo();

        // check table fileds count，只能处理加字段
        if (tableMeta != null && columnInfo.length > tableMeta.getFileds().size()) {
            // online ddl增加字段操作步骤：
            // 1. 新增一张临时表，将需要做ddl表的数据全量导入
            // 2. 在老表上建立I/U/D的trigger，增量的将数据插入到临时表
            // 3. 锁住应用请求，将临时表rename为老表的名字，完成增加字段的操作
            // 尝试做一次reload，可能因为ddl没有正确解析，或者使用了类似online ddl的操作
            // 因为online ddl没有对应表名的alter语法，所以不会有clear cache的操作
            tableMeta = tableMetaCache.getTableMeta(event.getTable().getDbName(),
                    event.getTable().getTableName(),
                    false);// 强制重新获取一次
            if (tableMeta == null) {
                throw new EventParseException("not found [" + event.getTable().getDbName() + "."
                        + event.getTable().getTableName() + "] in db , pls check!");
            }

            // 在做一次判断
            if (tableMeta != null && columnInfo.length > tableMeta.getFileds().size()) {
                throw new EventParseException("column size is not match for table:" + tableMeta.getFullName() + ","
                        + columnInfo.length + " vs " + tableMeta.getFileds().size());
            }
        }

        for (int i = 0; i < columnCnt; i++) {

            TableMapLogEvent.ColumnInfo info = columnInfo[i];
            EventEntry.Column.Builder columnBuilder = EventEntry.Column.newBuilder();

            FieldMeta fieldMeta = null;
            if (tableMeta != null) {
                // 处理file meta
                fieldMeta = tableMeta.getFileds().get(i);
                columnBuilder.setName(fieldMeta.getColumnName());
                columnBuilder.setIsKey(fieldMeta.isKey());
                columnBuilder.setMysqlType(fieldMeta.getColumnType()); // 增加mysql
                // type类型,issue
                // 73

                String dbName = event.getTable().getDbName();
                String tbName = event.getTable().getTableName();
                String fdName = fieldMeta.getColumnName();

                if(senseMap.size() > 0 && senseMap.containsKey(dbName + "." + tbName + "." + fdName)) {
                    continue;
                }
            }
            columnBuilder.setIndex(i);
            columnBuilder.setIsNull(false);

            // fixed issue
            // https://github.com/alibaba/canal/issues/66，特殊处理binary/varbinary，不能做编码处理
            boolean isBinary = false;
            if (fieldMeta != null) {
                if (StringUtils.containsIgnoreCase(fieldMeta.getColumnType(), "VARBINARY")) {
                    isBinary = true;
                } else if (StringUtils.containsIgnoreCase(fieldMeta.getColumnType(), "BINARY")) {
                    isBinary = true;
                }
            }
            buffer.nextValue(info.type, info.meta, isBinary);

            int javaType = buffer.getJavaType();
            if (buffer.isNull()) {
                columnBuilder.setIsNull(true);
            } else {
                final Serializable value = buffer.getValue();
                // 处理各种类型
                switch (javaType) {
                    case Types.INTEGER:
                    case Types.TINYINT:
                    case Types.SMALLINT:
                    case Types.BIGINT:
                        // 处理unsigned类型
                        Number number = (Number) value;
                        if (fieldMeta != null && fieldMeta.isUnsigned() && number.longValue() < 0) {
                            switch (buffer.getLength()) {
                                case 1: /* MYSQL_TYPE_TINY */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(TINYINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.SMALLINT; // 往上加一个量级
                                    break;

                                case 2: /* MYSQL_TYPE_SHORT */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(SMALLINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.INTEGER; // 往上加一个量级
                                    break;

                                case 3: /* MYSQL_TYPE_INT24 */
                                    columnBuilder.setValue(String.valueOf(Integer.valueOf(MEDIUMINT_MAX_VALUE
                                            + number.intValue())));
                                    javaType = Types.INTEGER; // 往上加一个量级
                                    break;

                                case 4: /* MYSQL_TYPE_LONG */
                                    columnBuilder.setValue(String.valueOf(Long.valueOf(INTEGER_MAX_VALUE
                                            + number.longValue())));
                                    javaType = Types.BIGINT; // 往上加一个量级
                                    break;

                                case 8: /* MYSQL_TYPE_LONGLONG */
                                    columnBuilder.setValue(BIGINT_MAX_VALUE.add(BigInteger.valueOf(number.longValue()))
                                            .toString());
                                    javaType = Types.DECIMAL; // 往上加一个量级，避免执行出错
                                    break;
                            }
                        } else {
                            // 对象为number类型，直接valueof即可
                            columnBuilder.setValue(String.valueOf(value));
                        }
                        break;
                    case Types.REAL: // float
                    case Types.DOUBLE: // double
                        // 对象为number类型，直接valueof即可
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.BIT:// bit
                        // 对象为number类型
                        columnBuilder.setValue(String.valueOf(value));
                        break;
                    case Types.DECIMAL:
                        columnBuilder.setValue(((BigDecimal) value).toPlainString());
                        break;
                    case Types.TIMESTAMP:
                        // 修复时间边界值
                        // String v = value.toString();
                        // v = v.substring(0, v.length() - 2);
                        // columnBuilder.setValue(v);
                        // break;
                    case Types.TIME:
                    case Types.DATE:
                        // 需要处理year
                        columnBuilder.setValue(value.toString());
                        break;
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                        // fixed text encoding
                        // https://github.com/AlibabaTech/canal/issues/18
                        // mysql binlog中blob/text都处理为blob类型，需要反查table
                        // meta，按编码解析text
                        if (fieldMeta != null && isText(fieldMeta.getColumnType())) {
                            columnBuilder.setValue(new String((byte[]) value, charset));
                            javaType = Types.CLOB;
                        } else {
                            // byte数组，直接使用iso-8859-1保留对应编码，浪费内存
                            columnBuilder.setValue(new String((byte[]) value, ISO_8859_1));
                            javaType = Types.BLOB;
                        }
                        break;
                    case Types.CHAR:
                    case Types.VARCHAR:
                        columnBuilder.setValue(value.toString());
                        break;
                    default:
                        columnBuilder.setValue(value.toString());
                }

            }

            columnBuilder.setSqlType(javaType);
            // 设置是否update的标记位
            columnBuilder.setUpdated(isAfter
                    && isUpdate(rowDataBuilder.getBeforeColumnsList(),
                    columnBuilder.getIsNull() ? null : columnBuilder.getValue(),
                    i));
            if (isAfter) {
                rowDataBuilder.addAfterColumns(columnBuilder.build());
            } else {
                rowDataBuilder.addBeforeColumns(columnBuilder.build());
            }
        }

    }

    private EventEntry.Entry buildQueryEntry(String queryString, LogHeader logHeader) {
        EventEntry.Header header = createHeader(binlogFileName, logHeader, "", "", EventEntry.EventType.QUERY);
        EventEntry.RowChange.Builder rowChangeBuider = EventEntry.RowChange.newBuilder();
        rowChangeBuider.setSql(queryString);
        rowChangeBuider.setEventType(EventEntry.EventType.QUERY);
        return createEntry(header, EventEntry.EntryType.ROWDATA, rowChangeBuider.build().toByteString());
    }

    private EventEntry.Header createHeader(String binlogFile, LogHeader logHeader, String schemaName, String tableName,
                                           EventEntry.EventType eventType) {
        // header会做信息冗余,方便以后做检索或者过滤
        EventEntry.Header.Builder headerBuilder = EventEntry.Header.newBuilder();
        headerBuilder.setVersion(version);
        headerBuilder.setLogfileName(binlogFile);
        headerBuilder.setLogfileOffset(logHeader.getLogPos() - logHeader.getEventLen());
        headerBuilder.setServerId(logHeader.getServerId());
        headerBuilder.setServerenCode(UTF_8);// 经过java输出后所有的编码为unicode
        headerBuilder.setExecuteTime(logHeader.getWhen() * 1000L);
        headerBuilder.setSourceType(EventEntry.Type.MYSQL);
        if (eventType != null) {
            headerBuilder.setEventType(eventType);
        }
        if (schemaName != null) {
            headerBuilder.setSchemaName(schemaName);
        }
        if (tableName != null) {
            headerBuilder.setTableName(tableName);
        }
        headerBuilder.setEventLength(logHeader.getEventLen());
        return headerBuilder.build();
    }

    private boolean isUpdate(List<EventEntry.Column> bfColumns, String newValue, int index) {
        if (bfColumns == null) {
            throw new EventParseException("ERROR ## the bfColumns is null");
        }

        if (index < 0) {
            return false;
        }
        if ((bfColumns.size() - 1) < index) {
            return false;
        }
        EventEntry.Column column = bfColumns.get(index);

        if (column.getIsNull()) {
            if (newValue != null) {
                return true;
            }
        } else {
            if (newValue == null) {
                return true;
            } else {
                if (!column.getValue().equals(newValue)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isText(String columnType) {
        return "LONGTEXT".equalsIgnoreCase(columnType) || "MEDIUMTEXT".equalsIgnoreCase(columnType)
                || "TEXT".equalsIgnoreCase(columnType) || "TINYTEXT".equalsIgnoreCase(columnType);
    }

    public static EventEntry.TransactionBegin createTransactionBegin(long threadId) {
        EventEntry.TransactionBegin.Builder beginBuilder = EventEntry.TransactionBegin.newBuilder();
        beginBuilder.setThreadId(threadId);
        return beginBuilder.build();
    }

    public static EventEntry.TransactionEnd createTransactionEnd(long transactionId) {
        EventEntry.TransactionEnd.Builder endBuilder = EventEntry.TransactionEnd.newBuilder();
        endBuilder.setTransactionId(String.valueOf(transactionId));
        return endBuilder.build();
    }

    public static EventEntry.Pair createSpecialPair(String key, String value) {
        EventEntry.Pair.Builder pairBuilder = EventEntry.Pair.newBuilder();
        pairBuilder.setKey(key);
        pairBuilder.setValue(value);
        return pairBuilder.build();
    }

    public static EventEntry.Entry createEntry(EventEntry.Header header, EventEntry.EntryType entryType, ByteString storeValue) {
        EventEntry.Entry.Builder entryBuilder = EventEntry.Entry.newBuilder();
        entryBuilder.setHeader(header);
        entryBuilder.setEntryType(entryType);
        entryBuilder.setStoreValue(storeValue);
        return entryBuilder.build();
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public void setNameFilter(AviaterRegexFilter nameFilter) {
        this.nameFilter = nameFilter;
    }

    public void setTableMetaCache(TableMetaCache tableMetaCache) {
        this.tableMetaCache = tableMetaCache;
    }

    public void setFilterQueryDcl(boolean filterQueryDcl) {
        this.filterQueryDcl = filterQueryDcl;
    }

    public void setFilterQueryDml(boolean filterQueryDml) {
        this.filterQueryDml = filterQueryDml;
    }

    public void setFilterQueryDdl(boolean filterQueryDdl) {
        this.filterQueryDdl = filterQueryDdl;
    }

}
