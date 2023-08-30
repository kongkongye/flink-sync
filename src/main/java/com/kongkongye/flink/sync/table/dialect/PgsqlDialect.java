package com.kongkongye.flink.sync.table.dialect;

import com.google.common.collect.Sets;
import com.kongkongye.flink.sync.util.SyncUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class PgsqlDialect extends AbstractJdbcDialect {
    public static final String NAME = "pgsql";
    public static final String Q = "'";

    public static final Set<String> QUOTE_TYPES = Sets.newHashSet(
            "date", "timestamp", "character",
            "varchar", "char", "text"
    );

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public String getQuote() {
        return "\"";
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:postgresql");
    }

    @Override
    public String getDatabase(String url) {
        return url.substring(url.lastIndexOf("/") + 1);
    }

    @Override
    public String getColumnTypeSql(String columnName) {
        return "SELECT DATA_TYPE\n" +
                "FROM information_schema.COLUMNS\n" +
                "WHERE\n" +
                "     TABLE_NAME   = '" + config.getTo().getTable() + "' AND\n" +
                "     COLUMN_NAME  = '" + columnName + "'";
    }

    @Override
    public String getColumnType(String dataType) {
        if (dataType.startsWith("timestamp")) {
            return "timestamp";
        }
        if (dataType.startsWith("character")) {
            return "character";
        }
        return dataType;
    }

    @Override
    public String getDriverName() {
        return "org.postgresql.Driver";
    }

    /**
     * @return insert into orders_result (order_no, user_id, user_name) values ('a005', 5, 'e') ON DUPLICATE KEY UPDATE user_id=1, user_name='ee'
     */
    @Override
    public String getUpsertSql() {
        List<String> allColumns = new ArrayList<>(config.getTo().getIdList());
        allColumns.addAll(config.getTo().getColumnList());
        return "insert into " + q(config.getTo().getTable()) + " ( " + SyncUtil.getFieldsStr(allColumns, getQuote()) + " ) values (" + SyncUtil.getPlaceholdersStr(allColumns.size()) + " ) "
                + " ON CONFLICT ("+SyncUtil.getFieldsStr(config.getTo().getIdList(), getQuote())+") do update " + SyncUtil.getFieldPlaceholdersStr(allColumns, ",", getQuote());
    }

    @Override
    public String getInsertIgnoreSql() {
        List<String> allColumns = new ArrayList<>(config.getTo().getIdList());
        allColumns.addAll(config.getTo().getColumnList());
        return "insert into " + q(config.getTo().getTable()) + " ( " + SyncUtil.getFieldsStr(allColumns, getQuote()) + " ) values (" + SyncUtil.getPlaceholdersStr(allColumns.size()) + ") ON CONFLICT DO NOTHING";
    }

    @Override
    public List<String> getInsertIgnoreColumns(List<String> idList, List<String> columnList) {
        List<String> params = new ArrayList<>();
        params.addAll(idList);
        params.addAll(columnList);
        return params;
    }

    @Override
    public List<String> getInsertColumns(List<String> idList, List<String> columnList) {
        List<String> params = new ArrayList<>();
        params.addAll(idList);
        params.addAll(columnList);
        return params;
    }

    @Override
    public List<String> getUpdateColumns(List<String> idList, List<String> columnList) {
        List<String> params = new ArrayList<>();
        params.addAll(columnList);
        params.addAll(idList);
        return params;
    }

    @Override
    public List<String> getDeleteColumns(List<String> idList, List<String> columnList) {
        List<String> params = new ArrayList<>();
        params.addAll(idList);
        return params;
    }

    @Override
    public List<String> getUpsertColumns(List<String> idList, List<String> columnList) {
        List<String> params = new ArrayList<>();
        for (int j = 0; j < 2; j++) {
            params.addAll(idList);
            params.addAll(columnList);
        }
        return params;
    }

    @Override
    @Nonnull
    public String wrapParameter(String dataType, @Nullable Object value) {
        if (value == null) {
            return "null";
        }

        if (QUOTE_TYPES.contains(dataType)) {
            //单引号变成两个
            value = value.toString().replace("'", "''");
            //包裹
            return Q + value + Q;
        }

        return value.toString();
    }
}
