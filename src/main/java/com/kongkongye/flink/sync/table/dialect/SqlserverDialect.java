package com.kongkongye.flink.sync.table.dialect;

import com.google.common.collect.Sets;
import com.kongkongye.flink.sync.patch.sqlserver.SqlServerDialect;
import com.kongkongye.flink.sync.table.config.AliasName;
import com.kongkongye.flink.sync.util.SyncUtil;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class SqlserverDialect extends AbstractJdbcDialect {
    public static final String NAME = "sqlserver";
    public static final String Q = "'";

    public static final Set<String> QUOTE_TYPES = Sets.newHashSet(
            "date", "time", "datetime",
            "varchar", "char", "text",
            "nvarchar", "nchar", "ntext",
            "decimal",
            "bit"
    );
    private SqlServerDialect sqlServerDialect = new SqlServerDialect(true);

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
        return url.startsWith("jdbc:jtds:sqlserver");
    }

    @Override
    public String getDatabase(String url) {
        return url.substring(url.lastIndexOf("/") + 1);
    }

    @Override
    public String getColumnTypeSql(String columnName) {
        return "SELECT DATA_TYPE\n" +
                "FROM INFORMATION_SCHEMA.COLUMNS\n" +
                "WHERE\n" +
                "     TABLE_SCHEMA = '" + config.getTo().getSchema() + "' AND\n" +
                "     TABLE_NAME   = '" + config.getTo().getTable() + "' AND\n" +
                "     COLUMN_NAME  = '" + columnName + "'";
    }

    @Override
    public String getDriverName() {
        return "net.sourceforge.jtds.jdbc.Driver";
    }

    @Override
    public String getUpsertSql() {
        List<AliasName> allColumns = new ArrayList<>(config.getTo().getIdList());
        allColumns.addAll(config.getTo().getColumnList());
        List<String> allColumnsTo = allColumns.stream().map(AliasName::getAlias).collect(Collectors.toList());
        List<String> idListTo = config.getTo().getIdList().stream().map(AliasName::getAlias).collect(Collectors.toList());
        Optional<String> upsertStatement = sqlServerDialect.getUpsertStatement(config.getTo().getTable(), allColumnsTo.toArray(new String[0]), idListTo.toArray(new String[0]));
        return upsertStatement.orElse(null);
    }

    @Override
    public String getInsertIgnoreSql() {
        List<AliasName> allColumns = new ArrayList<>(config.getTo().getIdList());
        allColumns.addAll(config.getTo().getColumnList());
        List<String> allColumnsTo = allColumns.stream().map(AliasName::getAlias).collect(Collectors.toList());
        List<String> idListTo = config.getTo().getIdList().stream().map(AliasName::getAlias).collect(Collectors.toList());
        return "insert into " + q(config.getTo().getTable()) + " ( " + SyncUtil.getFieldsStr(allColumnsTo, getQuote()) + " ) SELECT " + SyncUtil.getPlaceholdersStr(allColumns.size()) + " WHERE NOT EXISTS (SELECT 1 FROM " + q(config.getTo().getTable()) + " WHERE " + SyncUtil.getFieldPlaceholdersStr(idListTo, " and ", getQuote()) + ")";
    }

    @Override
    public List<AliasName> getInsertIgnoreColumns(List<AliasName> idList, List<AliasName> columnList) {
        List<AliasName> params = new ArrayList<>();
        params.addAll(idList);
        params.addAll(columnList);
        params.addAll(idList);
        return params;
    }

    @Override
    public List<AliasName> getInsertColumns(List<AliasName> idList, List<AliasName> columnList) {
        List<AliasName> params = new ArrayList<>();
        params.addAll(idList);
        params.addAll(columnList);
        return params;
    }

    @Override
    public List<AliasName> getUpdateColumns(List<AliasName> idList, List<AliasName> columnList) {
        List<AliasName> params = new ArrayList<>();
        params.addAll(columnList);
        params.addAll(idList);
        return params;
    }

    @Override
    public List<AliasName> getDeleteColumns(List<AliasName> idList, List<AliasName> columnList) {
        List<AliasName> params = new ArrayList<>();
        params.addAll(idList);
        return params;
    }

    @Override
    public List<AliasName> getUpsertColumns(List<AliasName> idList, List<AliasName> columnList) {
        List<AliasName> params = new ArrayList<>();
        params.addAll(idList);
        params.addAll(columnList);
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
