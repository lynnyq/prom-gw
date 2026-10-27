package com.lynnyq.promgw.dlq;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

/**
 * DlqReplayerConfig DLQ 重放工具配置。
 *
 * 加载 properties 配置文件,字段与 dlq-replayer.properties 一一对应。
 * 所有数值型配置均做合法性校验,非法值抛出 IllegalArgumentException。
 *
 * 表路由说明:
 *   - starrocks.table 为 fallback 表名,仅当 DLQ 消息中 table 字段为空时使用
 *   - sysid 分表模式下,DLQ 消息自带 table 字段,重放工具按表分桶写入
 */
public class DlqReplayerConfig {

    // ====== Kafka 配置 ======
    private final String kafkaBootstrapServers;
    private final String kafkaDlqTopic;
    private final String kafkaConsumerGroupId;
    private final String kafkaAutoOffsetReset;
    private final boolean kafkaEnableAutoCommit;
    private final int kafkaMaxPollRecords;
    private final int kafkaSessionTimeoutMs;

    // ====== StarRocks 配置 ======
    private final String srFeHost;
    private final int srFePort;
    private final String srDb;
    /** fallback 表名:DLQ 消息中 table 为空时使用 */
    private final String srTableFallback;
    private final String srUser;
    private final String srPassword;
    private final boolean srGzip;

    // ====== 重试配置 ======
    private final int replayMaxRetry;
    private final long replayBackoffBaseMs;
    private final long replayBackoffMaxMs;
    private final double replayBackoffMultiplier;

    // ====== 攒批配置 ======
    private final int batchSize;
    private final long batchWaitMs;

    // ====== HTTP 配置 ======
    private final int httpConnectTimeoutMs;
    private final int httpSocketTimeoutMs;

    // ====== 告警配置 ======
    private final boolean alertEnabled;
    private final String alertWebhook;
    private final long alertIntervalMs;

    /**
     * properties key → 环境变量名 映射。
     * systemd 通过 EnvironmentFile 注入环境变量,优先级高于 properties 文件。
     */
    private static final Map<String, String> ENV_OVERRIDE = Map.ofEntries(
            Map.entry("kafka.bootstrap.servers", "DLQ_KAFKA_BROKERS"),
            Map.entry("kafka.dlq.topic", "DLQ_KAFKA_TOPIC"),
            Map.entry("kafka.consumer.group.id", "DLQ_KAFKA_GROUP_ID"),
            Map.entry("kafka.auto.offset.reset", "DLQ_KAFKA_AUTO_OFFSET_RESET"),
            Map.entry("kafka.enable.auto.commit", "DLQ_KAFKA_ENABLE_AUTO_COMMIT"),
            Map.entry("kafka.max.poll.records", "DLQ_KAFKA_MAX_POLL_RECORDS"),
            Map.entry("kafka.session.timeout.ms", "DLQ_KAFKA_SESSION_TIMEOUT_MS"),
            Map.entry("starrocks.fe.host", "DLQ_SR_HOST"),
            Map.entry("starrocks.fe.port", "DLQ_SR_PORT"),
            Map.entry("starrocks.db", "DLQ_SR_DB"),
            Map.entry("starrocks.table", "DLQ_SR_TABLE"),
            Map.entry("starrocks.user", "DLQ_SR_USER"),
            Map.entry("starrocks.password", "DLQ_SR_PASSWORD"),
            Map.entry("starrocks.gzip", "DLQ_SR_GZIP"),
            Map.entry("replay.max.retry", "DLQ_MAX_RETRY"),
            Map.entry("replay.backoff.base.ms", "DLQ_BACKOFF_BASE_MS"),
            Map.entry("replay.backoff.max.ms", "DLQ_BACKOFF_MAX_MS"),
            Map.entry("replay.backoff.multiplier", "DLQ_BACKOFF_MULTIPLIER"),
            Map.entry("batch.size", "DLQ_BATCH_SIZE"),
            Map.entry("batch.wait.ms", "DLQ_BATCH_WAIT_MS"),
            Map.entry("http.connect.timeout.ms", "DLQ_HTTP_CONNECT_TIMEOUT_MS"),
            Map.entry("http.socket.timeout.ms", "DLQ_HTTP_SOCKET_TIMEOUT_MS"),
            Map.entry("alert.enabled", "DLQ_ALERT_ENABLED"),
            Map.entry("alert.webhook", "DLQ_ALERT_WEBHOOK"),
            Map.entry("alert.interval.ms", "DLQ_ALERT_INTERVAL_MS"));

    /**
     * 从 properties 文件加载配置。
     *
     * 环境变量(systemd EnvironmentFile)优先级高于 properties 文件:
     * 若对应环境变量存在且非空,则覆盖 properties 中的值。
     *
     * @param configPath properties 文件路径
     * @throws IOException           文件读取失败
     * @throws IllegalArgumentException 必填字段缺失或数值非法
     */
    public DlqReplayerConfig(String configPath) throws IOException {
        Properties props = new Properties();
        try (InputStream in = new FileInputStream(configPath)) {
            props.load(in);
        }

        // 环境变量覆盖(优先于 properties 文件)
        for (Map.Entry<String, String> e : ENV_OVERRIDE.entrySet()) {
            String envVal = System.getenv(e.getValue());
            if (envVal != null && !envVal.trim().isEmpty()) {
                props.setProperty(e.getKey(), envVal.trim());
            }
        }

        // Kafka
        this.kafkaBootstrapServers = require(props, "kafka.bootstrap.servers");
        this.kafkaDlqTopic = require(props, "kafka.dlq.topic");
        this.kafkaConsumerGroupId = require(props, "kafka.consumer.group.id");
        this.kafkaAutoOffsetReset = props.getProperty("kafka.auto.offset.reset", "earliest");
        this.kafkaEnableAutoCommit = getBool(props, "kafka.enable.auto.commit", false);
        this.kafkaMaxPollRecords = getInt(props, "kafka.max.poll.records", 500, 1, 10000);
        this.kafkaSessionTimeoutMs = getInt(props, "kafka.session.timeout.ms", 30000, 1000, 600000);

        // StarRocks
        this.srFeHost = require(props, "starrocks.fe.host");
        this.srFePort = getInt(props, "starrocks.fe.port", 8030, 1, 65535);
        this.srDb = require(props, "starrocks.db");
        this.srTableFallback = require(props, "starrocks.table");
        this.srUser = props.getProperty("starrocks.user", "root");
        this.srPassword = props.getProperty("starrocks.password", "");
        this.srGzip = getBool(props, "starrocks.gzip", true);

        // 重试
        this.replayMaxRetry = getInt(props, "replay.max.retry", 5, 0, 100);
        this.replayBackoffBaseMs = getLong(props, "replay.backoff.base.ms", 10000L, 1L, 3600_000L);
        this.replayBackoffMaxMs = getLong(props, "replay.backoff.max.ms", 300000L, 1L, 3600_000L * 24L);
        this.replayBackoffMultiplier = getDouble(props, "replay.backoff.multiplier", 2.0, 1.0, 10.0);

        // 攒批
        this.batchSize = getInt(props, "batch.size", 100, 1, 10000);
        this.batchWaitMs = getLong(props, "batch.wait.ms", 5000L, 100L, 600000L);

        // HTTP
        this.httpConnectTimeoutMs = getInt(props, "http.connect.timeout.ms", 60000, 1000, 600000);
        this.httpSocketTimeoutMs = getInt(props, "http.socket.timeout.ms", 60000, 1000, 600000);

        // 告警
        this.alertEnabled = getBool(props, "alert.enabled", true);
        this.alertWebhook = props.getProperty("alert.webhook", "");
        this.alertIntervalMs = getLong(props, "alert.interval.ms", 300000L, 1000L, 3600_000L * 24L);
    }

    /** require 读取必填字符串字段,缺失或为空抛 IllegalArgumentException。 */
    private static String require(Properties props, String key) {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            throw new IllegalArgumentException("missing required config: " + key);
        }
        return v.trim();
    }

    private static boolean getBool(Properties props, String key, boolean def) {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        return Boolean.parseBoolean(v.trim());
    }

    private static int getInt(Properties props, String key, int def, int min, int max) {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        int val;
        try {
            val = Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid int for " + key + ": " + v, e);
        }
        if (val < min || val > max) {
            throw new IllegalArgumentException(key + " out of range [" + min + ", " + max + "]: " + val);
        }
        return val;
    }

    private static long getLong(Properties props, String key, long def, long min, long max) {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        long val;
        try {
            val = Long.parseLong(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid long for " + key + ": " + v, e);
        }
        if (val < min || val > max) {
            throw new IllegalArgumentException(key + " out of range [" + min + ", " + max + "]: " + val);
        }
        return val;
    }

    private static double getDouble(Properties props, String key, double def, double min, double max) {
        String v = props.getProperty(key);
        if (v == null || v.trim().isEmpty()) {
            return def;
        }
        double val;
        try {
            val = Double.parseDouble(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid double for " + key + ": " + v, e);
        }
        if (val < min || val > max) {
            throw new IllegalArgumentException(key + " out of range [" + min + ", " + max + "]: " + val);
        }
        return val;
    }

    // ====== Getters ======

    public String getKafkaBootstrapServers() { return kafkaBootstrapServers; }
    public String getKafkaDlqTopic() { return kafkaDlqTopic; }
    public String getKafkaConsumerGroupId() { return kafkaConsumerGroupId; }
    public String getKafkaAutoOffsetReset() { return kafkaAutoOffsetReset; }
    public boolean isKafkaEnableAutoCommit() { return kafkaEnableAutoCommit; }
    public int getKafkaMaxPollRecords() { return kafkaMaxPollRecords; }
    public int getKafkaSessionTimeoutMs() { return kafkaSessionTimeoutMs; }

    public String getSrFeHost() { return srFeHost; }
    public int getSrFePort() { return srFePort; }
    public String getSrDb() { return srDb; }
    public String getSrTableFallback() { return srTableFallback; }
    public String getSrUser() { return srUser; }
    public String getSrPassword() { return srPassword; }
    public boolean isSrGzip() { return srGzip; }

    public int getReplayMaxRetry() { return replayMaxRetry; }
    public long getReplayBackoffBaseMs() { return replayBackoffBaseMs; }
    public long getReplayBackoffMaxMs() { return replayBackoffMaxMs; }
    public double getReplayBackoffMultiplier() { return replayBackoffMultiplier; }

    public int getBatchSize() { return batchSize; }
    public long getBatchWaitMs() { return batchWaitMs; }

    public int getHttpConnectTimeoutMs() { return httpConnectTimeoutMs; }
    public int getHttpSocketTimeoutMs() { return httpSocketTimeoutMs; }

    public boolean isAlertEnabled() { return alertEnabled; }
    public String getAlertWebhook() { return alertWebhook; }
    public long getAlertIntervalMs() { return alertIntervalMs; }
}
