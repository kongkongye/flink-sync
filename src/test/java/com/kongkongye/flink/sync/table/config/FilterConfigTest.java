package com.kongkongye.flink.sync.table.config;

import org.junit.Assert;
import org.junit.Test;

public class FilterConfigTest {
    @Test
    public void shouldMatchEq() {
        FilterConfig config = new FilterConfig();
        config.setColumn("type");
        config.setOp("eq");
        config.setValue("lens");
        config.after();

        Assert.assertTrue(config.match("lens"));
        Assert.assertFalse(config.match("frame"));
    }

    @Test
    public void shouldMatchIn() {
        FilterConfig config = new FilterConfig();
        config.setColumn("type");
        config.setOp("in");
        config.setValue("lens,frame");
        config.after();

        Assert.assertTrue(config.match("lens"));
        Assert.assertTrue(config.match("frame"));
        Assert.assertFalse(config.match("other"));
    }

    @Test
    public void shouldMatchIsNull() {
        FilterConfig config = new FilterConfig();
        config.setColumn("remark");
        config.setOp("isNull");
        config.after();

        Assert.assertTrue(config.match(null));
        Assert.assertFalse(config.match("x"));
    }

    @Test
    public void shouldMatchIsNotNull() {
        FilterConfig config = new FilterConfig();
        config.setColumn("remark");
        config.setOp("isNotNull");
        config.after();

        Assert.assertFalse(config.match(null));
        Assert.assertTrue(config.match("x"));
    }

    @Test
    public void shouldMatchStartsWith() {
        FilterConfig config = new FilterConfig();
        config.setColumn("orderNo");
        config.setOp("startsWith");
        config.setValue("SD");
        config.after();

        Assert.assertTrue(config.match("SD20240601"));
        Assert.assertFalse(config.match("WX20240601"));
    }

    @Test
    public void shouldMatchNotStartsWith() {
        FilterConfig config = new FilterConfig();
        config.setColumn("orderNo");
        config.setOp("notStartsWith");
        config.setValue("SD");
        config.after();

        Assert.assertFalse(config.match("SD20240601"));
        Assert.assertTrue(config.match("WX20240601"));
    }
}
