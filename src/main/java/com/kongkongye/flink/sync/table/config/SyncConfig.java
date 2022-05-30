package com.kongkongye.flink.sync.table.config;

import com.alibaba.fastjson2.JSON;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;

@Slf4j
@Data
public class SyncConfig implements Serializable{
    private FromConfig from;
    private ToConfig to;

    /**
     * 读取配置并进行初始化
     */
    public static SyncConfig load(String jsonString) {
        SyncConfig syncConfig = JSON.parseObject(jsonString, SyncConfig.class);
        syncConfig.init();
        return syncConfig;
    }

    private void init() {
        to.init();
    }
}
