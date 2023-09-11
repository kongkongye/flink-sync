package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

/**
 * 0E-8 => 0.00000000
 */
public class BigDecimalFixConverter implements Converter<Object, String> {
    @Override
    public String name() {
        return "bigDecimalFix";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return value instanceof String && "decimal".equalsIgnoreCase(dataType);
    }

    @Override
    public String convert(JSONObject config, Object input) {
        //config
        int scale = config.getIntValue("scale");//保留小数位数

        String str = input.toString();
        return new java.math.BigDecimal(str).setScale(scale, java.math.BigDecimal.ROUND_HALF_UP).toPlainString();
    }
}
