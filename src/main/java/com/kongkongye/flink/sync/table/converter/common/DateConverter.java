package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.TimeZone;

/**
 * day => "yyyy-MM-dd"
 */
public class DateConverter implements Converter<Object, String> {
    @Override
    public String name() {
        return "date";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return "date".equalsIgnoreCase(dataType);
    }

    @Override
    public String convert(JSONObject config, Object input) {
        //config
        long offset = config.getLongValue("offset", 0L);//偏移，可为负，目的是将时间调整为标准的utc+0时间，单位ms
        String timezone = config.getString("timezone");//时区

        int day = Integer.parseInt(input.toString());
        TimeZone timeZone = TimeZone.getTimeZone(timezone);
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.setTimeInMillis(0);
        calendar.add(Calendar.DATE, day);
        calendar.add(Calendar.MILLISECOND, (int)offset);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(calendar.getTime());
    }
}
