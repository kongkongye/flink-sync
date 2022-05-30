package com.kongkongye.flink.sync.table.dialect;

import com.google.common.collect.Sets;
import com.kongkongye.flink.sync.util.SyncUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class MysqlDialect extends AbstractJdbcDialect {
    public static final String NAME = "mysql";
    public static final String Q = "'";

    public static final Set<String> QUOTE_TYPES = Sets.newHashSet(
            "date", "datetime", "timestamp",
            "varchar", "char", "text"
    );

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public boolean canHandle(String url) {
        return url.startsWith("jdbc:mysql");
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
    public String getDriverName() {
        return "com.mysql.cj.jdbc.Driver";
    }

    /**
     * @return insert into orders_result (order_no, user_id, user_name) values ('a005', 5, 'e') ON DUPLICATE KEY UPDATE user_id=1, user_name='ee'
     */
    @Override
    public String getUpsertSql() {
        List<String> allColumns = new ArrayList<>(config.getTo().getIdList());
        allColumns.addAll(config.getTo().getColumnList());
        return "insert into " + config.getTo().getTable() + " ( " + SyncUtil.getFieldsStr(allColumns) + " ) values (" + SyncUtil.getPlaceholdersStr(allColumns.size()) + " ) "
                + " ON DUPLICATE KEY UPDATE " + SyncUtil.getFieldPlaceholdersStr(allColumns, ",");
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
