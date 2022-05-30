/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.kongkongye.flink.sync.patch.sqlserver;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.jdbc.converter.JdbcRowConverter;
import org.apache.flink.connector.jdbc.dialect.AbstractDialect;
import org.apache.flink.table.types.logical.LogicalTypeRoot;
import org.apache.flink.table.types.logical.RowType;

import java.util.*;
import java.util.stream.Collectors;

/**
 * JDBC dialect for sqlserver.
 */
@Internal
public class SqlServerDialect extends AbstractDialect {
    private static final long serialVersionUID = 1L;
    private static final String SQL_DEFAULT_PLACEHOLDER = " :";

    // Define MAX/MIN precision of TIMESTAMP type according to sqlserver docs:
    // https://dev.sqlserver.com/doc/refman/8.0/en/fractional-seconds.html
    private static final int MAX_TIMESTAMP_PRECISION = 6;
    private static final int MIN_TIMESTAMP_PRECISION = 0;

    // Define MAX/MIN precision of DECIMAL type according to sqlserver docs:
    // https://dev.sqlserver.com/doc/refman/8.0/en/fixed-point-types.html
    private static final int MAX_DECIMAL_PRECISION = 65;
    private static final int MIN_DECIMAL_PRECISION = 1;

    private boolean useIndexParameter = false;

    public SqlServerDialect() {
    }

    public SqlServerDialect(boolean useIndexParameter) {
        this.useIndexParameter = useIndexParameter;
    }

    @Override
    public JdbcRowConverter getRowConverter(RowType rowType) {
        return new SQLServerRowConverter(rowType);
    }

    /**
     * 未实现，好像实现不了？直接返回空字符串表示没效果
     */
    @Override
    public String getLimitClause(long limit) {
        throw new UnsupportedOperationException("SqlServerDialect.getLimitClause() not implemented");
    }

    @Override
    public Optional<String> defaultDriverName() {
        return Optional.of("net.sourceforge.jtds.jdbc.Driver");
    }

    @Override
    public String quoteIdentifier(String identifier) {
        return "[" + identifier + "]";
    }

    @Override
    public Optional<String> getUpsertStatement(
            String tableName, String[] fieldNames, String[] uniqueKeyFields) {
        //merge存在死锁问题，暂时用回fallback模式
        if (true) {
            return Optional.empty();
        }

        StringBuilder mergeIntoSql = new StringBuilder();

        mergeIntoSql

                .append("MERGE INTO " + tableName + " WITH (serializable) AS T1 USING (")

                .append(buildDualQueryStatement(fieldNames))

                .append(") AS T2 ON (")

                .append(buildConnectionConditions(uniqueKeyFields) + ") ");


        String updateSql = buildUpdateConnection(fieldNames, uniqueKeyFields);


        if (StringUtils.isNotEmpty(updateSql)) {

            mergeIntoSql.append(" WHEN MATCHED THEN UPDATE SET ");

            mergeIntoSql.append(updateSql);

        }


        mergeIntoSql

                .append(" WHEN NOT MATCHED THEN ")

                .append("INSERT (")

                .append(

                        Arrays.stream(fieldNames)

                                .map(col -> quoteIdentifier(col))

                                .collect(Collectors.joining(",")))

                .append(") VALUES (")

                .append(

                        Arrays.stream(fieldNames)

                                .map(col -> "T2." + quoteIdentifier(col))

                                .collect(Collectors.joining(",")))

                .append(");");

        return Optional.of(mergeIntoSql.toString());
    }

    @Override
    public String dialectName() {
        return "SQLServer";
    }

    @Override
    public Optional<Range> decimalPrecisionRange() {
        return Optional.of(Range.of(MIN_DECIMAL_PRECISION, MAX_DECIMAL_PRECISION));
    }

    @Override
    public Optional<Range> timestampPrecisionRange() {
        return Optional.of(Range.of(MIN_TIMESTAMP_PRECISION, MAX_TIMESTAMP_PRECISION));
    }

    @Override
    public Set<LogicalTypeRoot> supportedTypes() {
        // The data types used in Mysql are list at:
        // https://dev.mysql.com/doc/refman/8.0/en/data-types.html

        // TODO: We can't convert BINARY data type to
        //  PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO in
        // LegacyTypeInfoDataTypeConverter.
        return EnumSet.of(
                LogicalTypeRoot.CHAR,
                LogicalTypeRoot.VARCHAR,
                LogicalTypeRoot.BOOLEAN,
                LogicalTypeRoot.VARBINARY,
                LogicalTypeRoot.DECIMAL,
                LogicalTypeRoot.TINYINT,
                LogicalTypeRoot.SMALLINT,
                LogicalTypeRoot.INTEGER,
                LogicalTypeRoot.BIGINT,
                LogicalTypeRoot.FLOAT,
                LogicalTypeRoot.DOUBLE,
                LogicalTypeRoot.DATE,
                LogicalTypeRoot.TIME_WITHOUT_TIME_ZONE,
                LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE);
    }

    public String buildDualQueryStatement(String[] column) {

        StringBuilder sb = new StringBuilder("SELECT ");

        String collect =

                Arrays.stream(column)

                        .map(

                                col ->

                                        (useIndexParameter ? "?" : wrapperPlaceholder(col))

//                                                + quoteIdentifier(col)

                                                + " AS "

                                                + quoteIdentifier(col))

                        .collect(Collectors.joining(", "));

        return sb.toString() + collect;

    }

    public String wrapperPlaceholder(String fieldName) {

        return SQL_DEFAULT_PLACEHOLDER + fieldName + " ";

    }

    private String buildConnectionConditions(String[] uniqueKeyFields) {

        return Arrays.stream(uniqueKeyFields)

                .map(

                        col ->

                                "T1."

                                        + quoteIdentifier(col.trim())

                                        + "=T2."

                                        + quoteIdentifier(col.trim()))

                .collect(Collectors.joining(" and "));

    }

    private String buildUpdateConnection(String[] fieldNames, String[] uniqueKeyFields) {

        List<String> uniqueKeyList = Arrays.asList(uniqueKeyFields);

        String updateConnectionSql =

                Arrays.stream(fieldNames)

                        .filter(

                                col -> {

                                    boolean bbool =

                                            uniqueKeyList.contains(col.toLowerCase())

                                                    || uniqueKeyList.contains(

                                                    col.toUpperCase())

                                                    ? false

                                                    : true;

                                    return bbool;

                                })

                        .map(

                                col ->

                                        quoteIdentifier("T1")

                                                + "."

                                                + quoteIdentifier(col)

                                                + " = "

                                                + quoteIdentifier("T2")

                                                + "."

                                                + quoteIdentifier(col))

                        .collect(Collectors.joining(","));

        return updateConnectionSql;

    }
}
