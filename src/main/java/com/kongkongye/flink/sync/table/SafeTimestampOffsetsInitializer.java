package com.kongkongye.flink.sync.table;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetAndTimestamp;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class SafeTimestampOffsetsInitializer implements OffsetsInitializer {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(SafeTimestampOffsetsInitializer.class);

    private final long startingTimestamp;

    public SafeTimestampOffsetsInitializer(long startingTimestamp) {
        this.startingTimestamp = startingTimestamp;
    }

    @Override
    public Map<TopicPartition, Long> getPartitionOffsets(
            Collection<TopicPartition> partitions,
            PartitionOffsetsRetriever partitionOffsetsRetriever) {
        Map<TopicPartition, Long> initialOffsets = new HashMap<>();
        Map<TopicPartition, Long> endOffsets = partitionOffsetsRetriever.endOffsets(partitions);

        for (TopicPartition tp : partitions) {
            try {
                Map<TopicPartition, OffsetAndTimestamp> offsetsForTimes =
                        partitionOffsetsRetriever.offsetsForTimes(
                                Collections.singletonMap(tp, startingTimestamp));
                OffsetAndTimestamp offsetAndTimestamp = offsetsForTimes.get(tp);
                if (offsetAndTimestamp != null) {
                    initialOffsets.put(tp, offsetAndTimestamp.offset());
                } else {
                    initialOffsets.put(tp, endOffsets.get(tp));
                }
            } catch (IllegalArgumentException e) {
                Long fallbackOffset = endOffsets.get(tp);
                log.warn(
                        "Resolve timestamp offset failed, fallback to latest. topicPartition={}, timestamp={}, latestOffset={}",
                        tp,
                        startingTimestamp,
                        fallbackOffset,
                        e);
                initialOffsets.put(tp, fallbackOffset);
            }
        }

        return initialOffsets;
    }

    @Override
    public OffsetResetStrategy getAutoOffsetResetStrategy() {
        return OffsetResetStrategy.LATEST;
    }
}
