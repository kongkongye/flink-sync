package com.kongkongye.flink.sync.table.dialect;

import com.kongkongye.flink.sync.table.config.AliasName;
import com.kongkongye.flink.sync.table.config.SyncConfig;
import com.kongkongye.flink.sync.util.SyncUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class AbstractJdbcDialect implements JdbcDialect {
    protected SyncConfig config;

    @Override
    public void init(SyncConfig config) {
        this.config = config;
    }

    /**
     * @return insert into orders_result (order_no, user_id, user_name) values ('a005', 5, 'e')
     */
    @Override
    public String getInsertSql() {
        List<AliasName> allColumns = new ArrayList<>(config.getTo().getIdList());
        allColumns.addAll(config.getTo().getColumnList());
        List<String> allColumnsTo = allColumns.stream().map(AliasName::getAlias).collect(Collectors.toList());
        return "insert into " + q(config.getTo().getTable()) + " ( " + SyncUtil.getFieldsStr(allColumnsTo, getQuote()) + " ) values (" + SyncUtil.getPlaceholdersStr(allColumnsTo.size()) + ")";
    }

    /**
     * @return update orders_result set user_name='ee' where order_no='a005' and user_id=5
     */
    @Override
    public String getUpdateSql() {
        List<String> idListTo = config.getTo().getIdList().stream().map(AliasName::getAlias).collect(Collectors.toList());
        List<String> columnListTo = config.getTo().getColumnList().stream().map(AliasName::getAlias).collect(Collectors.toList());
        return "update " + q(config.getTo().getTable()) + " set " + SyncUtil.getFieldPlaceholdersStr(columnListTo, ",", getQuote()) + " where " + SyncUtil.getFieldPlaceholdersStr(idListTo, " and ", getQuote());
    }

    /**
     * @return delete from orders_result where order_no='a005' and user_id=5
     */
    @Override
    public String getDeleteSql() {
        List<String> idListTo = config.getTo().getIdList().stream().map(AliasName::getAlias).collect(Collectors.toList());
        return "delete from " + q(config.getTo().getTable()) + " where " + SyncUtil.getFieldPlaceholdersStr(idListTo, " and ", getQuote());
    }

    /**
     * @return insert into orders_result (order_no, user_id, user_name) values ('a005', 5, 'e') ON DUPLICATE KEY UPDATE user_id=1, user_name='ee'
     */
    @Override
    public String getUpsertSql() {
        return null;
    }
}
