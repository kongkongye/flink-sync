package com.kongkongye.flink.sync.table.dialect;

import com.kongkongye.flink.sync.table.config.SyncConfig;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Serializable;
import java.util.List;

public interface JdbcDialect extends Serializable {
    /**
     * 初始化
     */
    void init(SyncConfig config);

    /**
     * 方言名称
     */
    String getName();

    /**
     * 获取对象引号
     */
    String getQuote();

    /**
     * 能否处理该url
     */
    boolean canHandle(String url);

    /**
     * 获取url里包含的数据库名
     */
    String getDatabase(String url);

    /**
     * 获取驱动器名称
     */
    String getDriverName();

    /**
     * 获取列类型的sql
     */
    String getColumnTypeSql(String columnName);

    /**
     * 获取列类型
     * @param dataType 数据库返回的类型值
     */
    default String getColumnType(String dataType) {
        return dataType;
    }

    /**
     * 带有占位符的插入语句
     */
    String getInsertSql();

    /**
     * 带有占位符的更新语句
     */
    String getUpdateSql();

    /**
     * 带有占位符的删除语句
     */
    String getDeleteSql();

    /**
     * 带有占位符的upsert语句
     *
     * @return 返回null表示不支持
     */
    @Nullable
    String getUpsertSql();

    String getInsertIgnoreSql();

    List<String> getInsertColumns(List<String> idList, List<String> columnList);

    List<String> getInsertIgnoreColumns(List<String> idList, List<String> columnList);

    List<String> getUpdateColumns(List<String> idList, List<String> columnList);

    List<String> getDeleteColumns(List<String> idList, List<String> columnList);

    List<String> getUpsertColumns(List<String> idList, List<String> columnList);

    /**
     * 包裹变量
     *
     * @param dataType sql数据类型
     */
    @Nonnull
    String wrapParameter(String dataType, @Nullable Object value);

    default String q(String objectName) {
        return getQuote() + objectName + getQuote();
    }
}
