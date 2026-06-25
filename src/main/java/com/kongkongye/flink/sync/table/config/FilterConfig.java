package com.kongkongye.flink.sync.table.config;

import lombok.Data;

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

@Data
public class FilterConfig implements Serializable {
    private String column;
    private String op;
    private Object value;

    private Set<String> valueSet = new HashSet<>();

    public void after() {
        if (column != null) {
            column = column.trim();
        }
        if (op != null) {
            op = op.trim();
        }
        if ("in".equalsIgnoreCase(op) && value != null) {
            if (value instanceof Iterable) {
                for (Object item : (Iterable<?>) value) {
                    valueSet.add(String.valueOf(item));
                }
            } else {
                valueSet = Arrays.stream(String.valueOf(value).split(","))
                        .map(String::trim)
                        .filter(e -> !e.isEmpty())
                        .collect(Collectors.toSet());
            }
        }
    }

    public boolean match(Object actualValue) {
        String actual = String.valueOf(actualValue);
        if ("isNull".equalsIgnoreCase(op)) {
            return actualValue == null;
        }
        if ("isNotNull".equalsIgnoreCase(op)) {
            return actualValue != null;
        }
        if ("eq".equalsIgnoreCase(op)) {
            return Objects.equals(actual, String.valueOf(value));
        }
        if ("ne".equalsIgnoreCase(op)) {
            return !Objects.equals(actual, String.valueOf(value));
        }
        if ("in".equalsIgnoreCase(op)) {
            return valueSet.contains(actual);
        }
        throw new RuntimeException("unsupported filter op: " + op);
    }
}
