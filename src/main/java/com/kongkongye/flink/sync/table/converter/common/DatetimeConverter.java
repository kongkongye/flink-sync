package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * ms时间戳 => "yyyy-MM-dd HH:mm:ss.SSS"
 */
public class DatetimeConverter implements Converter<Long, String> {
    @Override
    public String name() {
        return "datetime";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return value instanceof Long && "datetime".equalsIgnoreCase(dataType);
    }

    @Override
    public String convert(JSONObject config, Long input) {
        //config
        long offset = config.getLongValue("offset", 0L);//偏移，可为负，目的是将时间调整为标准的utc+0时间，单位ms
        String timezone = config.getString("timezone");//时区

        TimeZone timeZone = TimeZone.getTimeZone(timezone);
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTimeInMillis(input+offset);
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(calendar.getTime());
    }
}