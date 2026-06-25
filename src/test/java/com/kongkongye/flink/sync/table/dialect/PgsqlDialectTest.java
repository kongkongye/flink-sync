package com.kongkongye.flink.sync.table.dialect;

import com.kongkongye.flink.sync.table.config.FromConfig;
import com.kongkongye.flink.sync.table.config.SyncConfig;
import com.kongkongye.flink.sync.table.config.ToConfig;
import org.junit.Assert;
import org.junit.Test;

public class PgsqlDialectTest {
    @Test
    public void shouldIncludeSchemaInColumnTypeSql() {
        SyncConfig config = new SyncConfig();
        config.setFrom(new FromConfig());
        ToConfig to = new ToConfig();
        to.setSchema("ods");
        to.setTable("orders");
        to.setIds("id");
        to.setColumns("name");
        to.init();
        config.setTo(to);

        PgsqlDialect dialect = new PgsqlDialect();
        dialect.init(config);

        String sql = dialect.getColumnTypeSql("create_dt");
        Assert.assertTrue(sql.contains("TABLE_SCHEMA = 'ods'"));
        Assert.assertTrue(sql.contains("TABLE_NAME   = 'orders'"));
        Assert.assertTrue(sql.contains("COLUMN_NAME  = 'create_dt'"));
    }
}
