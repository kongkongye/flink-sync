package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

/**
 * 空值时补默认值
 */
public class DefaultValueConverter implements Converter<Object, Object> {
    @Override
    public String name() {
        return "defaultValue";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        if (value == null) {
            return true;
        }
        if (value instanceof String) {
            return ((String) value).trim().isEmpty();
        }
        return false;
    }

    @Override
    public boolean canHandle(JSONObject config, String dataType, Object value) {
        boolean applyOnNull = config.getBooleanValue("applyOnNull", true);
        boolean applyOnBlank = config.getBooleanValue("applyOnBlank", true);

        if (value == null) {
            return applyOnNull;
        }
        if (value instanceof String && ((String) value).trim().isEmpty()) {
            return applyOnBlank;
        }
        return false;
    }

    @Override
    public Object convert(JSONObject config, Object input) {
        return config.get("value");
    }
}
