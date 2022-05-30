package com.kongkongye.flink.sync.table.config;

import com.kongkongye.flink.sync.table.config.enums.FromType;
import com.kongkongye.flink.sync.table.config.enums.StartMethod;
import lombok.Data;

import java.io.Serializable;
import java.util.Map;

@Data
public class FromConfig implements Serializable {
    private FromType type;

    //kafka
    private String bootstrapServers;
    private String topic;
    private StartMethod startMethod;
    private Long startMethodTimestamp;

    //cdc_mysql && cdc_sqlserver
    private String hostname;
    private Integer port;
    private String username;
    private String password;
    private String database;
    private String table;
    private String serverTimeZone;
    private String startupOptions;
    private Map<String, String> debezium;

//    public StartupOptions loadMysqlStartupOptions() {
//        switch (startupOptions) {
//            case "initial":
//                return StartupOptions.initial();
//            case "earliest":
//                return StartupOptions.earliest();
//            case "latest":
//                return StartupOptions.latest();
//            default:
//                throw new RuntimeException("unknown startupOptions: " + startupOptions);
//        }
//    }

//    public com.ververica.cdc.connectors.sqlserver.table.StartupOptions loadSqlserverStartupOptions() {
//        switch (startupOptions) {
//            case "initial":
//                return com.ververica.cdc.connectors.sqlserver.table.StartupOptions.initial();
//            case "initialOnly":
//                return com.ververica.cdc.connectors.sqlserver.table.StartupOptions.initialOnly();
//            case "latest":
//                return com.ververica.cdc.connectors.sqlserver.table.StartupOptions.latest();
//            default:
//                throw new RuntimeException("unknown startupOptions: " + startupOptions);
//        }
//    }
}