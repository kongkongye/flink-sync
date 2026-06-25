package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class DefaultValueConverterTest {
    @Test
    public void shouldHandleNullAndBlank() {
        DefaultValueConverter converter = new DefaultValueConverter();

        Assert.assertTrue(converter.canHandle("varchar", null));
        Assert.assertTrue(converter.canHandle("varchar", "   "));
        Assert.assertFalse(converter.canHandle("varchar", "abc"));
    }

    @Test
    public void shouldReturnConfiguredDefaultValue() {
        DefaultValueConverter converter = new DefaultValueConverter();
        JSONObject config = new JSONObject();
        config.put("value", "UNKNOWN");

        Assert.assertEquals("UNKNOWN", converter.convert(config, null));
    }

    @Test
    public void shouldSupportOnlyNullMode() {
        DefaultValueConverter converter = new DefaultValueConverter();
        JSONObject config = new JSONObject();
        config.put("value", "UNKNOWN");
        config.put("applyOnNull", true);
        config.put("applyOnBlank", false);

        Assert.assertTrue(converter.canHandle(config, "varchar", null));
        Assert.assertFalse(converter.canHandle(config, "varchar", "   "));
    }
}
