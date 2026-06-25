package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * ms时间戳 => "yyyy-MM-dd"
 */
public class TimestampDateConverter implements Converter<Number, String> {
    @Override
    public String name() {
        return "timestampDate";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return value instanceof Number && "date".equalsIgnoreCase(dataType);
    }

    @Override
    public String convert(JSONObject config, Number input) {
        long offset = config.getLongValue("offset", 0L);
        String timezone = config.getString("timezone");

        TimeZone timeZone = TimeZone.getTimeZone(timezone);
        Calendar calendar = Calendar.getInstance(timeZone);
        long pre13 = input.longValue();
        if (String.valueOf(pre13).length() > 13) {
            String tmp = String.valueOf(pre13).substring(0, 13);
            pre13 = Long.parseLong(tmp);
        }
        calendar.setTimeInMillis(pre13 + offset);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(calendar.getTime());
    }
}
