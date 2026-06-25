package com.kongkongye.flink.sync.table.converter;

import com.kongkongye.flink.sync.table.converter.common.BigDecimalFixConverter;
import com.kongkongye.flink.sync.table.converter.common.DateConverter;
import com.kongkongye.flink.sync.table.converter.common.DefaultValueConverter;
import com.kongkongye.flink.sync.table.converter.common.DatetimeConverter;
import com.kongkongye.flink.sync.table.converter.common.StringDatetimeConverter;
import com.kongkongye.flink.sync.table.converter.common.ValueMapConverter;
import com.kongkongye.flink.sync.table.converter.common.ZonedDatetimeStrConverter;

import java.util.HashMap;
import java.util.Map;

public class Converters {
    //converterName Converter
    public static Map<String, Converter<?, ?>> converters = new HashMap<>();

    static {
        register(new DatetimeConverter());
        register(new StringDatetimeConverter());
        register(new ZonedDatetimeStrConverter());
        register(new DateConverter());
        register(new BigDecimalFixConverter());
        register(new ValueMapConverter());
        register(new DefaultValueConverter());
    }

    public static void register(Converter<?, ?> converter) {
        converters.put(converter.name(), converter);

    }

    public static Converter<?, ?> getByName(String name) {
        return converters.get(name);
    }
}
