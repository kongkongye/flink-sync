package com.kongkongye.flink.sync.table.converter;

import com.kongkongye.flink.sync.table.converter.common.BigDecimalFixConverter;
import com.kongkongye.flink.sync.table.converter.common.DateConverter;
import com.kongkongye.flink.sync.table.converter.common.DatetimeConverter;

import java.util.HashMap;
import java.util.Map;

public class Converters {
    //converterName Converter
    public static Map<String, Converter<?, ?>> converters = new HashMap<>();

    static {
        register(new DatetimeConverter());
        register(new DateConverter());
        register(new BigDecimalFixConverter());
    }

    public static void register(Converter<?, ?> converter) {
        converters.put(converter.name(), converter);

    }

    public static Converter<?, ?> getByName(String name) {
        return converters.get(name);
    }
}
