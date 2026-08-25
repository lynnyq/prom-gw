package com.example.promgw;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * JobConfigTest 验证 Kafka offset 相关参数的命令行解析与默认值。
 */
class JobConfigTest {

    @Test
    void testDefaults() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{});
        assertThat(cfg.kafkaStartFrom).isEqualTo("committed");
        assertThat(cfg.kafkaOffsetReset).isEqualTo("latest");
        assertThat(cfg.kafkaStartTimestamp).isEqualTo(0L);
    }

    @Test
    void testParseStartFromEarliest() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{"--kafka-start-from", "earliest"});
        assertThat(cfg.kafkaStartFrom).isEqualTo("earliest");
    }

    @Test
    void testParseStartFromLatest() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{"--kafka-start-from", "latest"});
        assertThat(cfg.kafkaStartFrom).isEqualTo("latest");
    }

    @Test
    void testParseStartFromTimestamp() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{
                "--kafka-start-from", "timestamp",
                "--kafka-start-timestamp", "1700000000000"
        });
        assertThat(cfg.kafkaStartFrom).isEqualTo("timestamp");
        assertThat(cfg.kafkaStartTimestamp).isEqualTo(1700000000000L);
    }

    @Test
    void testParseOffsetReset() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{"--kafka-offset-reset", "earliest"});
        assertThat(cfg.kafkaOffsetReset).isEqualTo("earliest");
    }

    @Test
    void testParseOffsetResetNone() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{"--kafka-offset-reset", "none"});
        assertThat(cfg.kafkaOffsetReset).isEqualTo("none");
    }

    @Test
    void testParseAllOffsetParamsTogether() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{
                "--kafka-start-from", "timestamp",
                "--kafka-offset-reset", "earliest",
                "--kafka-start-timestamp", "1700000000000"
        });
        assertThat(cfg.kafkaStartFrom).isEqualTo("timestamp");
        assertThat(cfg.kafkaOffsetReset).isEqualTo("earliest");
        assertThat(cfg.kafkaStartTimestamp).isEqualTo(1700000000000L);
    }

    @Test
    void testToStringContainsOffsetInfo() {
        JobConfig cfg = JobConfig.fromArgs(new String[]{
                "--kafka-start-from", "earliest"
        });
        String str = cfg.toString();
        assertThat(str).contains("kafkaStartFrom='earliest'");
        assertThat(str).contains("kafkaOffsetReset='latest'");
    }
}
