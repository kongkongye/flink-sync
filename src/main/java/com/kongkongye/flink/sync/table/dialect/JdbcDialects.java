package com.kongkongye.flink.sync.table.dialect;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

public class JdbcDialects {
    private static final Map<String, JdbcDialect> dialectMap = new HashMap<>();

    static {
        register(new MysqlDialect());
        register(new PgsqlDialect());
        register(new SqlserverDialect());
    }

    private static void register(JdbcDialect dialect) {
        dialectMap.put(dialect.getName(), dialect);
    }

    @Nullable
    public static JdbcDialect getJdbcDialect(String url) {
        for (Map.Entry<String, JdbcDialect> entry : dialectMap.entrySet()) {
            if (entry.getValue().canHandle(url)) {
                return entry.getValue();
            }
        }
        return null;
    }
}
