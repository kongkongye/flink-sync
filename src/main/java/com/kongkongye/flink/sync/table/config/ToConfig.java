package com.kongkongye.flink.sync.table.config;

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
    private List<ConverterConfig> converters = new ArrayList<>();

    private List<String> idList;
    private List<String> columnList;

    //列名 数据库类型
    private Map<String, String> types = new HashMap<>();

    public void init() {
        idList = Arrays.stream(ids.split(",")).map(String::trim).collect(Collectors.toList());
        columnList = Arrays.stream(columns.split(",")).map(String::trim).collect(Collectors.toList());
    }

    @SneakyThrows
    public void afterDialectLoaded(JdbcDialect dialect) {
        Class.forName(dialect.getDriverName());

        for (ConverterConfig converterConfig : converters) {
            converterConfig.after();
        }

        ArrayList<String> columns = new ArrayList<>(idList);
        columns.addAll(columnList);
        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            for (String column : columns) {
                String columnTypeSql = dialect.getColumnTypeSql(column);
                try (PreparedStatement ps = connection.prepareStatement(columnTypeSql)) {
                    try (ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            String columnType = rs.getString(1);
                            types.put(column, dialect.getColumnType(columnType));
                        }
                    }
                }
            }
        }
    }
}
