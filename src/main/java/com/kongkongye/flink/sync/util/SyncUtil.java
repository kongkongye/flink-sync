package com.kongkongye.flink.sync.util;

import org.apache.commons.compress.utils.Lists;

import java.util.List;
import java.util.stream.Collectors;

public class SyncUtil {
    /**
     * 获取fields字符串
     *
     * @return id, name
     */
    public static String getFieldsStr(List<String> columns) {
        return String.join(",", columns);
    }

    /**
     * 获取占位符字符串
     *
     * @return ?,?
     */
    public static String getPlaceholdersStr(int placeholderAmount) {
        //得到指定数量的列表
        List<String> placeholderList = Lists.newArrayList();
        for (int i = 0; i < placeholderAmount; i++) {
            placeholderList.add("?");
        }
        return String.join(",", placeholderList);
    }

    /**
     * 获取field placeholder字符串
     *
     * @return id=?,name=?
     */
    public static String getFieldPlaceholdersStr(List<String> columns, String separator) {
        List<String> placeholderList = columns.stream().map(e -> e+"=?").collect(Collectors.toList());
        return String.join(separator, placeholderList);
    }
}
