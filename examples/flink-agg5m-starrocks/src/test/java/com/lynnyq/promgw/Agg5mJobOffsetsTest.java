package com.lynnyq.promgw;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.junit.jupiter.api.Test;

/**
 * Agg5mJobOffsetsTest 验证 Kafka 起始位点策略构建逻辑。
 *
 * 覆盖 buildOffsetsInitializer 对 committed/earliest/latest/timestamp 的解析,
 * 以及非法取值和 timestamp 校验的异常分支。
 */
class Agg5mJobOffsetsTest {

    private static JobConfig cfgWith(String startFrom, String reset, long timestamp) {
        JobConfig cfg = new JobConfig();
        cfg.kafkaStartFrom = startFrom;
        cfg.kafkaOffsetReset = reset;
        cfg.kafkaStartTimestamp = timestamp;
        return cfg;
    }

    @Test
    void testCommittedLatest() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("committed", "latest", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testCommittedEarliest() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("committed", "earliest", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testCommittedNone() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("committed", "none", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testCommittedDefaultReset() {
        // 空 reset 应回退到 latest
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("committed", "", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testEarliest() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("earliest", "latest", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testLatest() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("latest", "latest", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testTimestamp() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("timestamp", "latest", 1700000000000L));
        assertThat(init).isNotNull();
    }

    @Test
    void testCaseInsensitive() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("EARLIEST", "LATEST", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testEmptyStartFromDefaultsToCommitted() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith("", "latest", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testNullStartFromDefaultsToCommitted() {
        OffsetsInitializer init = Agg5mJob.buildOffsetsInitializer(
                cfgWith(null, "latest", 0L));
        assertThat(init).isNotNull();
    }

    @Test
    void testTimestampZeroThrows() {
        assertThatThrownBy(() -> Agg5mJob.buildOffsetsInitializer(
                cfgWith("timestamp", "latest", 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--kafka-start-timestamp");
    }

    @Test
    void testTimestampNegativeThrows() {
        assertThatThrownBy(() -> Agg5mJob.buildOffsetsInitializer(
                cfgWith("timestamp", "latest", -1L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--kafka-start-timestamp");
    }

    @Test
    void testInvalidStartFromThrows() {
        assertThatThrownBy(() -> Agg5mJob.buildOffsetsInitializer(
                cfgWith("middle", "latest", 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--kafka-start-from")
                .hasMessageContaining("middle");
    }

    @Test
    void testInvalidOffsetResetThrows() {
        assertThatThrownBy(() -> Agg5mJob.buildOffsetsInitializer(
                cfgWith("committed", "bogus", 0L)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("--kafka-offset-reset")
                .hasMessageContaining("bogus");
    }

    @Test
    void testDifferentStrategiesReturnDistinctInstances() {
        OffsetsInitializer earliest = Agg5mJob.buildOffsetsInitializer(
                cfgWith("earliest", "latest", 0L));
        OffsetsInitializer latest = Agg5mJob.buildOffsetsInitializer(
                cfgWith("latest", "latest", 0L));
        OffsetsInitializer committed = Agg5mJob.buildOffsetsInitializer(
                cfgWith("committed", "latest", 0L));
        OffsetsInitializer timestamp = Agg5mJob.buildOffsetsInitializer(
                cfgWith("timestamp", "latest", 1700000000000L));
        // 不同策略不应返回完全相同的实例
        assertThat(earliest).isNotSameAs(latest);
        assertThat(latest).isNotSameAs(committed);
        assertThat(committed).isNotSameAs(timestamp);
    }
}
