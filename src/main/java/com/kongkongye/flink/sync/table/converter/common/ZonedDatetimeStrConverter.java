package com.kongkongye.flink.sync.table.converter.common;

import com.alibaba.fastjson2.JSONObject;
import com.kongkongye.flink.sync.table.converter.Converter;

import java.text.SimpleDateFormat;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.TimeZone;

/**
 * 带时区的字符串时间 => "yyyy-MM-dd HH:mm:ss.SSS"
 */
public class ZonedDatetimeStrConverter implements Converter<String, String> {
    @Override
    public String name() {
        return "zonedDatetimeStr";
    }

    @Override
    public boolean canHandle(String dataType, Object value) {
        return value instanceof String && ("datetime".equalsIgnoreCase(dataType) || "timestamp".equalsIgnoreCase(dataType));
    }

    @Override
    public String convert(JSONObject config, String input) {
        //config
        String timezone = config.getString("timezone");//目标时区
        String pattern = config.getString("pattern");
        if (pattern == null) {
            pattern = "yyyy-MM-dd'T'HH:mm:ss.SSSX";
        }

        // 定义DateTimeFormatter
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(pattern);
        // 解析日期字符串为ZonedDateTime
        ZonedDateTime zonedDateTime = ZonedDateTime.parse(input, formatter);

        // 从ZonedDateTime获取Instant和ZoneId
//        java.time.Instant instant = zonedDateTime.toInstant();
//        java.time.ZoneId zoneId = zonedDateTime.getZone();

        // 创建Calendar实例
        Calendar calendar = GregorianCalendar.from(zonedDateTime);
        //targetTimeZone
        TimeZone targetTimeZone = TimeZone.getTimeZone(timezone);
        calendar.setTimeZone(targetTimeZone);

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return sdf.format(calendar.getTime());
    }
}
