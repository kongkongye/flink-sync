package com.kongkongye.flink.sync.func;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.functions.ScalarFunction;

import java.math.BigDecimal;

@Slf4j
public class ToDecimalSafelyFunc extends ScalarFunction {
    @DataTypeHint("DECIMAL(18, 8)")
    public BigDecimal eval(String value, String defaultValueOnError) {
        try {
            return new BigDecimal(value);
        } catch (Exception e) {
            System.out.println("format error, return default value: " + defaultValueOnError);
            return new BigDecimal(defaultValueOnError);
        }
    }

    @DataTypeHint("DECIMAL(18, 8)")
    public BigDecimal eval(String value) {
        try {
            return new BigDecimal(value);
        } catch (Exception e) {
            System.out.println("format error, return default value: null");
            return null;
        }
    }
}
