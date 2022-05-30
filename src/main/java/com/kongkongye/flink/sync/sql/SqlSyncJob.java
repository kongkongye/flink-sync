package com.kongkongye.flink.sync.sql;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

public class SqlSyncJob {
    public static void main(String[] args) throws Exception {
        //获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String env = parameterTool.get("env", "prod");
        String file = parameterTool.get("file");

        //读取配置
        String content = FileUtils.readFileToString(new File(file), StandardCharsets.UTF_8);

        //执行sql
        StreamExecutionEnvironment environment;
        if (Objects.equals(env, "local")) environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        else environment = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.getConfig().getConfiguration().setString("pipeline.name", "[sql同步]" + file);

        String[] sqls = content.split("----");
        for (String sql : sqls) {
            tableEnvironment.executeSql(sql);
        }
    }
}
