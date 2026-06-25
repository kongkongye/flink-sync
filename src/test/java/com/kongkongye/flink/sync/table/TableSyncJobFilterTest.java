package com.kongkongye.flink.sync.table;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.config.FilterConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TableSyncJobFilterTest {
    @Test
    public void shouldMatchAfterFields() {
        FilterConfig filter = new FilterConfig();
        filter.setColumn("type");
        filter.setOp("eq");
        filter.setValue("lens");
        filter.after();

        JSONObject row = new JSONObject();
        JSONObject after = new JSONObject();
        after.put("type", "lens");
        row.put("after", after);

        Assert.assertTrue(TableSyncJob.matchFilters(Collections.singletonList(filter), row));
    }

    @Test
    public void shouldMatchBeforeFieldsForDelete() {
        FilterConfig filter = new FilterConfig();
        filter.setColumn("type");
        filter.setOp("eq");
        filter.setValue("lens");
        filter.after();

        JSONObject row = new JSONObject();
        JSONObject before = new JSONObject();
        before.put("type", "lens");
        row.put("before", before);

        Assert.assertTrue(TableSyncJob.matchFilters(Collections.singletonList(filter), row));
    }
}
