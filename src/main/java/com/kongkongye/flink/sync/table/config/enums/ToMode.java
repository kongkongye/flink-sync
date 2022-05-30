package com.kongkongye.flink.sync.table.config.enums;

public enum ToMode {
    /**
     * upsert语句，delete语句
     */
    upsert,
    /**
     * insert语句，delete语句
     * 对于新增与更新记录，先删除，再插入
     */
    retract,
    /**
     * insert语句，update语句，delete语句
     */
    plain,
    ;
}
