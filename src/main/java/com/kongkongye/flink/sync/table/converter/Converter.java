package com.kongkongye.flink.sync.table.converter;

import com.alibaba.fastjson2.JSONObject;

import javax.annotation.Nonnull;

public interface Converter<In, Out> {
    /**
     * 名称
     */
    String name();

    /**
     * @return 是否能处理
     */
    boolean canHandle(String dataType, Object value);

    /**
     * 转换
     */
    Out convert(@Nonnull JSONObject config, In input);
}
