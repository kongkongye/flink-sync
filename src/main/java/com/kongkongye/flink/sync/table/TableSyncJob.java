package com.kongkongye.flink.sync.table;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.kongkongye.flink.sync.table.config.AliasName;
import com.kongkongye.flink.sync.table.config.ConverterConfig;
import com.kongkongye.flink.sync.table.config.SyncConfig;
import com.kongkongye.flink.sync.table.config.enums.FromType;
import com.kongkongye.flink.sync.table.converter.Converter;
import com.kongkongye.flink.sync.table.converter.Converters;
import com.kongkongye.flink.sync.table.dialect.JdbcDialect;
import com.kongkongye.flink.sync.table.dialect.JdbcDialects;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.internal.GenericJdbcSinkFunction;
import org.apache.flink.connector.jdbc.internal.JdbcOutputFormat;
import org.apache.flink.connector.jdbc.internal.connection.SimpleJdbcConnectionProvider;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Objects;

@Slf4j
public class TableSyncJob {
    public static void main(String[] args) throws Exception {
        //获取参数
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String env = parameterTool.get("env", "prod");
        String file = parameterTool.get("file");

        //读取配置
        String content = FileUtils.readFileToString(new File(file), StandardCharsets.UTF_8);
        SyncConfig config = SyncConfig.load(content);
        //读取方言
        JdbcDialect dialect;
        {
            //加载sql方言
            dialect = JdbcDialects.getJdbcDialect(config.getTo().getUrl());
            Preconditions.checkNotNull(dialect, "url不支持：" + config.getTo().getUrl());
            log.info("dialect: {}", dialect.getClass().getName());
            //初始化sql方言
            dialect.init(config);
        }
        //方言加载后处理
        config.getTo().afterDialectLoaded(dialect);

        StreamExecutionEnvironment environment;
        if (Objects.equals(env, "local"))
            environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        else environment = StreamExecutionEnvironment.getExecutionEnvironment();

        JdbcExecutionOptions executionOptions = new JdbcExecutionOptions.Builder()
                .withBatchIntervalMs(1000)
                .withBatchSize(100)
                .withMaxRetries(3)
                .build();

        JdbcConnectionOptions connectionOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withUrl(config.getTo().getUrl())
                .withDriverName(dialect.getDriverName())
                .withUsername(config.getTo().getUsername())
                .withPassword(config.getTo().getPassword())
                .build();

        //1. source
        DataStream<String> sourceStream;//源流
        if (config.getFrom().getType() == FromType.kafka) {
            Source<Ele, ?, ?> source = KafkaSource.<Ele>builder()
                    .setBootstrapServers(config.getFrom().getBootstrapServers())
                    .setTopics(config.getFrom().getTopic())
                    .setGroupId(file)//用文件名当groupId
                    .setStartingOffsets(config.getFrom().getStartMethod().loadOffsetsInitializer(config.getFrom()))
                    .setDeserializer(new KafkaRecordDeserializationSchema<Ele>() {
                        @Override
                        public TypeInformation<Ele> getProducedType() {
                            return Types.POJO(Ele.class);
                        }

                        @Override
                        public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<Ele> out) {
                            //如果是墓碑事件，即消息体为空，则不处理
                            if (record.key() != null && record.value() != null) {
                                out.collect(new Ele(new String(record.key()), new String(record.value())));
                            }
                        }
                    })
                    .build();
            //上游并发数设置为1，避免乱序(而且实测发现从kafka拉数据很快，1个线程也没问题)
            SingleOutputStreamOperator<Ele> upstream = environment
                    .fromSource(source, WatermarkStrategy.noWatermarks(), config.getFrom().getType().name() + " source");
//            upstream.setParallelism(1);
            //keyBy一下，让相同key的消息被同一个线程处理
            KeyedStream<Ele, String> keyedStream = upstream.keyBy(Ele::getKey);
            //输出统一流
            sourceStream = keyedStream.map(Ele::getValue);
        }
//        else if (config.getFrom().getType() == FromType.cdc_mysql) {//todo
//            Properties debeziumProperties = new Properties();
//            if (config.getFrom().getDebezium() != null) {
//                for (Map.Entry<String, String> entry : config.getFrom().getDebezium().entrySet()) {
//                    debeziumProperties.put("debezium." + entry.getKey(), entry.getValue());
//                }
//            }
//            MySqlSource<String> source = MySqlSource.<String>builder()
//                    .hostname(config.getFrom().getHostname())
//                    .port(config.getFrom().getPort())
//                    .username(config.getFrom().getUsername())
//                    .password(config.getFrom().getPassword())
//                    .databaseList(config.getFrom().getDatabase())
//                    .tableList(config.getFrom().getTable())
//                    .serverTimeZone(config.getFrom().getServerTimeZone())
//                    .startupOptions(config.getFrom().loadMysqlStartupOptions())
//                    .debeziumProperties(debeziumProperties)
//                    .deserializer(new JsonDebeziumDeserializationSchema())
//                    .build();
//            sourceStream = environment.fromSource(source, WatermarkStrategy.noWatermarks(), config.getFrom().getType().name() + " source");
//        }
//        else if (config.getFrom().getType() == FromType.cdc_sqlserver) {//todo
//            Properties debeziumProperties = new Properties();
//            if (config.getFrom().getDebezium() != null) {
//                for (Map.Entry<String, String> entry : config.getFrom().getDebezium().entrySet()) {
//                    debeziumProperties.put("debezium." + entry.getKey(), entry.getValue());
//                }
//            }
//            DebeziumSourceFunction<String> source = SqlServerSource.<String>builder()
//                    .hostname(config.getFrom().getHostname())
//                    .port(config.getFrom().getPort())
//                    .username(config.getFrom().getUsername())
//                    .password(config.getFrom().getPassword())
//                    .database(config.getFrom().getDatabase())
//                    .tableList(config.getFrom().getTable())
//                    .startupOptions(config.getFrom().loadSqlserverStartupOptions())
//                    .debeziumProperties(debeziumProperties)
//                    .deserializer(new JsonDebeziumDeserializationSchema())
//                    .build();
//            sourceStream = environment.addSource(source, config.getFrom().getType().name() + " source");
//        }
        else {
            throw new RuntimeException("unsupported source type: " + config.getFrom().getType());
        }

        //2. transform

        //2.1 转json
        DataStream<JSONObject> jsonStream = sourceStream.map(JSON::parseObject).name("转json");
        //2.2 转换值
        SingleOutputStreamOperator<JSONObject> convertedStream = jsonStream.map(e -> {
            if (Objects.equals(e.getString("op"), "c") || Objects.equals(e.getString("op"), "r")) {//新增
                JSONObject after = e.getJSONObject("after");
                for (List<AliasName> list : Lists.newArrayList(config.getTo().getIdList(), config.getTo().getColumnList())) {
                    for (AliasName col : list) {
                        Object value = after.get(col.name);
                        value = convertValue(config.getTo().getConverters(), config.getTo().getTypes().get(col.alias), col.name, value);
                        after.put(col.name, value);
                    }
                }
            } else if (Objects.equals(e.getString("op"), "u")) {//更新
                JSONObject after = e.getJSONObject("after");
                for (List<AliasName> list : Lists.newArrayList(config.getTo().getIdList(), config.getTo().getColumnList())) {
                    for (AliasName col : list) {
                        Object value = after.get(col.name);
                        value = convertValue(config.getTo().getConverters(), config.getTo().getTypes().get(col.alias), col.name, value);
                        after.put(col.name, value);
                    }
                }
            } else if (Objects.equals(e.getString("op"), "d")) {//删除
                JSONObject before = e.getJSONObject("before");
                for (AliasName col : config.getTo().getIdList()) {
                    Object value = before.get(col.name);
                    value = convertValue(config.getTo().getConverters(), config.getTo().getTypes().get(col.alias), col.name, value);
                    before.put(col.name, value);
                }
            }
            return e;
        }).name("转换值");

        //3. sink
        convertedStream.addSink(new GenericJdbcSinkFunction<>(
                new JdbcOutputFormat<>(
                        new SimpleJdbcConnectionProvider(connectionOptions),
                        executionOptions,
                        context -> new SimpleBatchStatementExecutor(config, dialect),
                        JdbcOutputFormat.RecordExtractor.identity()))).name(dialect.getName() + " sink");

        //执行
        environment.execute("[表同步]" + file);
    }

    private static Object convertValue(List<ConverterConfig> converters, String columnType, String fromColumn, Object value) {
        if (value == null) {
            return null;
        }

        //按顺序遍历转换器配置
        for (ConverterConfig converterConfig : converters) {
            //检测列名匹配
            if (converterConfig.match(fromColumn)) {
                //获取转换器
                Converter converter = Converters.getByName(converterConfig.getConverter());
                if (converter == null) {
                    throw new RuntimeException("converter not found: " + converterConfig.getConverter());
                }
                //检测是否能处理
                if (converter.canHandle(columnType, value)) {
                    //转换
                    Object result = converter.convert(converterConfig.getConfig(), value);
                    log.debug("[convert]{}: {} --> {}", converter.name() + "|" + fromColumn, value, result);
                    //匹配到一个转换器，直接返回不再继续检测
                    return result;
                }
            }
        }

        return value;
    }
}
