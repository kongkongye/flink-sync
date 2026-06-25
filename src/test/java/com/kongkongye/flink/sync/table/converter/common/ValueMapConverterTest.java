package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import org.junit.Assert;
import org.junit.Test;

public class ValueMapConverterTest {
    @Test
    public void shouldMapValue() {
        ValueMapConverter converter = new ValueMapConverter();
        JSONObject config = new JSONObject();
        JSONObject mapping = new JSONObject();
        mapping.put("1", "lens");
        config.put("mapping", mapping);

        Assert.assertEquals("lens", converter.convert(config, 1));
        Assert.assertEquals("2", String.valueOf(converter.convert(config, 2)));
    }
}
