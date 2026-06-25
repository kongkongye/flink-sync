package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

/**
 * 按映射表替换值
 */
public class ValueMapConverter implements Converter<Object, Object> {
    @Override
    public String name() {
        return "valueMap";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return value != null;
    }

    @Override
    public Object convert(JSONObject config, Object input) {
        JSONObject mapping = config.getJSONObject("mapping");
        if (mapping == null) {
            return input;
        }
        return mapping.getOrDefault(String.valueOf(input), input);
    }
}
