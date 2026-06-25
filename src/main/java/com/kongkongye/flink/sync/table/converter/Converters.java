package com.kongkongye.flink.sync.table.converter;

import com.kongkongye.flink.sync.table.converter.common.*;

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
        register(new TimestampDateConverter());
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
