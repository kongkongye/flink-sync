package com.kongkongye.flink.sync.table.config.enums;

import com.kongkongye.flink.sync.table.config.FromConfig;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;

public enum StartMethod {
    earliest,
    latest,
    committedOffsets,
    committedOffsets_latest,
    committedOffsets_earliest,
    timestamp,
    ;

    public OffsetsInitializer loadOffsetsInitializer(FromConfig fromConfig) {
        switch (this) {
            case earliest:
                return OffsetsInitializer.earliest();
            case latest:
                return OffsetsInitializer.latest();
            case committedOffsets:
                return OffsetsInitializer.committedOffsets();
            case committedOffsets_latest:
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST);
            case committedOffsets_earliest:
                return OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST);
            case timestamp:
                return OffsetsInitializer.timestamp(fromConfig.getStartMethodTimestamp());
            default:
                throw new RuntimeException("unknown startMethod: " + this);
        }
    }
}
