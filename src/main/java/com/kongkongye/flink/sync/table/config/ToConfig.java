package com.kongkongye.flink.sync.table.config;

import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Preconditions;
import com.kongkongye.flink.sync.table.config.enums.ToMode;
import com.kongkongye.flink.sync.table.dialect.JdbcDialect;
import lombok.Data;
import lombok.SneakyThrows;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.*;
import java.util.stream.Collectors;

@Data
public class ToConfig implements Serializable {
    private ToMode mode = ToMode.plain;
    private String url;
    private String username;
    private String password;
    private String schema;
    private String table;
    private String ids;
    private String columns;
    private JSONObject extraParams;
    private List<ConverterConfig> converters = new ArrayList<>();

    private List<AliasName> idList;
    private List<AliasName> columnList;
    private Map<String, Object> extraParamsMap;

    //列名 数据库类型
    private Map<String, String> types = new HashMap<>();

    public void init() {
        idList = Arrays.stream(ids.split(",")).map(AliasName::of).collect(Collectors.toList());
        columnList = Arrays.stream(columns.split(",")).map(AliasName::of).collect(Collectors.toList());

        extraParamsMap = new HashMap<>();
        extraParams.forEach(
                (k, v) -> {
                    if (v instanceof JSONObject) {
                        String type = ((JSONObject) v).getString("type");
                        if ("now".equals(type)) {
                            v = System.currentTimeMillis();
                        }
                    }
                    extraParamsMap.put(k, v);
                }
        );
    }

    @SneakyThrows
    public void afterDialectLoaded(JdbcDialect dialect) {
        Class.forName(dialect.getDriverName());

        for (ConverterConfig converterConfig : converters) {
            converterConfig.after();
        }

        ArrayList<AliasName> columns = new ArrayList<>(idList);
        columns.addAll(columnList);
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            for (AliasName column : columns) {
                String columnTypeSql = dialect.getColumnTypeSql(column.alias);
                try (PreparedStatement ps = connection.prepareStatement(columnTypeSql)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String columnType = rs.getString(1);
                            Preconditions.checkNotNull(columnType, "columnType is null: " + column.alias);
                            types.put(column.alias, dialect.getColumnType(columnType));
                        }
                    }
                }
            }
        }
    }
}
