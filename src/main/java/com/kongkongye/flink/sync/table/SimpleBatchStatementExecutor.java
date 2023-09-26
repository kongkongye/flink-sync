package com.kongkongye.flink.sync.table;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.config.SyncConfig;
import com.kongkongye.flink.sync.table.dialect.JdbcDialect;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.connector.jdbc.internal.executor.JdbcBatchStatementExecutor;

import javax.annotation.Nullable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class SimpleBatchStatementExecutor implements JdbcBatchStatementExecutor<JSONObject> {
    private final SyncConfig config;
    private final JdbcDialect dialect;
    private final List<JSONObject> batch;
    private transient Statement statement;

    public SimpleBatchStatementExecutor(SyncConfig config, JdbcDialect dialect) {
        this.config = config;
        this.dialect = dialect;
        this.batch = new ArrayList<>();
    }

    @Override
    public void prepareStatements(Connection connection) throws SQLException {
        this.statement = connection.createStatement();
    }

    @Override
    public void addToBatch(JSONObject record) {
        batch.add(record);
    }

    @Override
    public void executeBatch() throws SQLException {
        try {
            if (!batch.isEmpty()) {
                for (JSONObject e : batch) {
                    //构建sql语句
                    for (String sql : buildSqls(e)) {
                        //添加batch
                        statement.addBatch(sql);
    //                    log.debug("[sql]{}", sql);
                    }
                }
                statement.executeBatch();
                batch.clear();
            }
        } catch (Exception e) {
            log.error("executeBatch error", e);
            throw e;
        }
    }

    @Override
    public void closeStatements() throws SQLException {
        if (statement != null) {
            statement.close();
            statement = null;
        }
    }

    /**
     * 获取插入语句
     */
    private String getInsertSql(JSONObject e) {
        String sqlWithPlaceholders = dialect.getInsertSql();

        JSONObject after = e.getJSONObject("after");

        //params
        List<String> params = new ArrayList<>();
        for (String column : dialect.getInsertColumns(config.getTo().getIdList(), config.getTo().getColumnList())) {
            Object value = after.get(column);
            String dataType = config.getTo().getTypes().get(column);
            String wrappedParameter = dialect.wrapParameter(dataType, value);
            params.add(wrappedParameter);
        }

        //填充变量
        return fillParams(sqlWithPlaceholders, params);
    }

    /**
     * 获取更新语句
     */
    private String getUpdateSql(JSONObject e) {
        String sqlWithPlaceholders = dialect.getUpdateSql();

        JSONObject after = e.getJSONObject("after");

        //params
        List<String> params = new ArrayList<>();
        for (String column : dialect.getUpdateColumns(config.getTo().getIdList(), config.getTo().getColumnList())) {
            Object value = after.get(column);
            String dataType = config.getTo().getTypes().get(column);
            String wrappedParameter = dialect.wrapParameter(dataType, value);
            params.add(wrappedParameter);
        }

        //填充变量
        return fillParams(sqlWithPlaceholders, params);
    }

    /**
     * 获取插件忽略错误语句
     */
    private String getInsertIgnoreSql(JSONObject e) {
        String sqlWithPlaceholders = dialect.getInsertIgnoreSql();

        JSONObject after = e.getJSONObject("after");

        //params
        List<String> params = new ArrayList<>();
        for (String column : dialect.getInsertIgnoreColumns(config.getTo().getIdList(), config.getTo().getColumnList())) {
            Object value = after.get(column);
            String dataType = config.getTo().getTypes().get(column);
            String wrappedParameter = dialect.wrapParameter(dataType, value);
            params.add(wrappedParameter);
        }

        //填充变量
        return fillParams(sqlWithPlaceholders, params);
    }

    /**
     * 获取删除语句
     */
    private String getDeleteSql(JSONObject e, boolean before) {
        String sqlWithPlaceholders = dialect.getDeleteSql();

        JSONObject json = e.getJSONObject(before ? "before" : "after");

        //params
        List<String> params = new ArrayList<>();
        for (String column : dialect.getDeleteColumns(config.getTo().getIdList(), config.getTo().getColumnList())) {
            Object value = json.get(column);
            String dataType = config.getTo().getTypes().get(column);
            String wrappedParameter = dialect.wrapParameter(dataType, value);
            params.add(wrappedParameter);
        }

        //填充变量
        return fillParams(sqlWithPlaceholders, params);
    }

    /**
     * 获取upsert语句
     */
    @Nullable
    private String getUpsertSql(JSONObject e) {
        String sqlWithPlaceholders = dialect.getUpsertSql();
        if (sqlWithPlaceholders == null) {
            return null;
        }

        JSONObject after = e.getJSONObject("after");

        //params
        List<String> params = new ArrayList<>();
        for (String column : dialect.getUpsertColumns(config.getTo().getIdList(), config.getTo().getColumnList())) {
            Object value = after.get(column);
            String dataType = config.getTo().getTypes().get(column);
            String wrappedParameter = dialect.wrapParameter(dataType, value);
            params.add(wrappedParameter);
        }

        //填充变量
        return fillParams(sqlWithPlaceholders, params);
    }

    /**
     * 填充变量
     * 就是把?用实际变量替换
     */
    private String fillParams(String sql, List<String> params) {
        Pattern pattern = Pattern.compile("\\?");
        Matcher matcher = pattern.matcher(sql);
        StringBuffer sb = new StringBuffer();
        int i = 0;
        while (matcher.find()) {
            matcher.appendReplacement(sb, Matcher.quoteReplacement(params.get(i)));
            i++;
        }
        matcher.appendTail(sb);
        return sb.toString();
    }

    /**
     * 构建sql语句
     */
    private List<String> buildSqls(JSONObject e) {
        List<String> sqls = new ArrayList<>();
        if (Objects.equals(e.getString("op"), "c") || Objects.equals(e.getString("op"), "r")) {//新增
            switch (config.getTo().getMode()) {
                case plain:
                    sqls.add(getInsertSql(e));
                    break;
                case insertIgnore:
                    sqls.add(getInsertIgnoreSql(e));
                    break;
                case upsert:
                    String upsertSql = getUpsertSql(e);
                    if (upsertSql == null) {
                        throw new RuntimeException("upsert mode is not supported");
                    }
                    sqls.add(upsertSql);
                    break;
                case retract:
                    sqls.add(getDeleteSql(e, false));
                    sqls.add(getInsertSql(e));
                    break;
                case insertUpdate:
                    sqls.add(getInsertIgnoreSql(e));
                    sqls.add(getUpdateSql(e));
                    break;
                default:
                    throw new RuntimeException("unsupported mode: " + config.getTo().getMode());
            }
        } else if (Objects.equals(e.getString("op"), "u")) {//更新
            switch (config.getTo().getMode()) {
                case plain:
                    sqls.add(getUpdateSql(e));
                    break;
                case insertIgnore:
                    sqls.add(getUpdateSql(e));
                    break;
                case upsert:
                    String upsertSql = getUpsertSql(e);
                    if (upsertSql == null) {
                        throw new RuntimeException("upsert mode is not supported");
                    }
                    sqls.add(upsertSql);
                    break;
                case retract:
                    sqls.add(getDeleteSql(e, false));
                    sqls.add(getInsertSql(e));
                    break;
                case insertUpdate:
                    sqls.add(getInsertIgnoreSql(e));
                    sqls.add(getUpdateSql(e));
                    break;
                default:
                    throw new RuntimeException("unsupported mode: " + config.getTo().getMode());
            }
        } else if (Objects.equals(e.getString("op"), "d")) {//删除
            sqls.add(getDeleteSql(e, true));
        }
        return sqls;
    }
}
