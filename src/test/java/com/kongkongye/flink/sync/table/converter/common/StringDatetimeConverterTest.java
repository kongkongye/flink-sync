package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class StringDatetimeConverterTest {
    @Test
    public void shouldConvertStringTimestamp() {
        StringDatetimeConverter converter = new StringDatetimeConverter();
        JSONObject config = new JSONObject();
        config.put("timezone", "UTC");
        config.put("offset", 0L);

        String result = converter.convert(config, "1704067200000");
        Assert.assertEquals("2024-01-01 08:00:00.000", result);
    }
}
