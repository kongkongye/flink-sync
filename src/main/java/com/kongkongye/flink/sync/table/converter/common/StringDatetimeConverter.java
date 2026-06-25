package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * 字符串数字时间戳 => yyyy-MM-dd HH:mm:ss.SSS
 */
public class StringDatetimeConverter implements Converter<String, String> {
    @Override
    public String name() {
        return "stringDatetime";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return value instanceof String && ((String) value).matches("\\d{10,}")
                && ("datetime".equalsIgnoreCase(dataType) || "timestamp".equalsIgnoreCase(dataType));
    }

    @Override
    public String convert(JSONObject config, String input) {
        long offset = config.getLongValue("offset", 0L);
        String timezone = config.getString("timezone");

        TimeZone timeZone = TimeZone.getTimeZone(timezone);
        Calendar calendar = Calendar.getInstance(timeZone);
        long pre13 = Long.parseLong(input);
        if (String.valueOf(pre13).length() > 13) {
            String tmp = String.valueOf(pre13).substring(0, 13);
            pre13 = Long.parseLong(tmp);
        }
        calendar.setTimeInMillis(pre13 + offset);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(calendar.getTime());
    }
}
