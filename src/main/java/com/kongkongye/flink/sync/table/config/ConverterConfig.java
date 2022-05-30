package com.kongkongye.flink.sync.table.config;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class ConverterConfig implements Serializable {
    /**
     * 星号(*)代表所有字段
     * 多个列名用逗号(,)分隔
     */
    private String columns;
    /**
     * 转换器名称
     */
    private String converter;
    /**
     * 转换器配置
     */
    private JSONObject config = new JSONObject();

    //缓存一些东西，加快效率
    private boolean all;
    private Set<String> columnSet = new HashSet<>();

    public void after() {
        columns = columns.trim();
        if (columns.equals("*")) {
            all = true;
        }else {
            columnSet = Arrays.stream(columns.split(",")).map(String::trim).collect(Collectors.toSet());
        }
    }

    /**
     * @param column 列名
     * @return 是否匹配
     */
    public boolean match(String column) {
        if (all) {
            return true;
        }else {
            return columnSet.contains(column);
        }
    }
}
