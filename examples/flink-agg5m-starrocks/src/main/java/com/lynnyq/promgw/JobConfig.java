package com.lynnyq.promgw;

import java.util.HashMap;
import java.util.Map;

/**
 * JobConfig 作业配置,从命令行参数解析。
 *
 * 支持 --env local / prod 两套预设,以及逐项覆盖。
 */
public class JobConfig {

    public String env = "local";

    // Kafka
    public String kafkaBrokers = "localhost:9092";
    public String kafkaTopic = "prom.local.routed.app_business";
    /**
     * 多 topic 列表(由 --topics 逗号分隔解析)。
     * 若配置了 --topics,则优先使用;否则使用 kafkaTopic 单 topic。
     * sysid 路由模式下,prom-gw 按 sysid 把数据写入不同 topic(如 prom.<city>.routed.s001),
     * Flink 需同时消费所有 sysid 对应的 topic。
     */
    public String[] kafkaTopics = null;
    public String kafkaGroupId = "flink-agg5m-local";
    public String kafkaSecurityProtocol = "";      // 空 = PLAINTEXT
    public String kafkaSaslMechanism = "";
    public String kafkaSaslJaasConfig = "";
    public String kafkaSslTruststoreLocation = "";
    public String kafkaSslTruststorePassword = "";
    public int sourceParallelism = 4;

    // Kafka offset 起始策略:committed(默认,从已提交位点)/earliest/latest/timestamp
    public String kafkaStartFrom = "committed";
    // 无已提交位点时的重置策略:earliest/latest/none(默认 latest)
    public String kafkaOffsetReset = "latest";
    // kafkaStartFrom=timestamp 时使用的起始时间戳(epoch millis)
    public long kafkaStartTimestamp = 0L;

    // StarRocks
    // srPort 默认 8030(FE http_port),StarRocks Stream Load 复用 FE HTTP 端口,
    // FE 收到请求后会 307 redirect 到 BE 的 be_http_port(8040)。
    // 不要用 8070 — 部署文档曾误标为 "Stream Load 端口",实际从未配置监听。
    public String srHost = "localhost";
    public int srPort = 8030;
    public String srDb = "observability";
    public String srTable = "metrics_5m";
    public String srUser = "root";
    public String srPassword = "";
    public boolean srGzip = true;
    public String srLabelPrefix = "local_5m";
    // StarRocks 攒批配置
    public int srBatchSize = 500;               // 攒批行数上限,达到即 flush
    public long srBatchIntervalMs = 10_000L;    // 攒批时间上限(ms),超时即 flush

    // sysid 表路由(核心特性)
    // sysidTableMap: sysid → StarRocks 表名 映射
    //   通过 --sysid-table-mapping "s001=metrics_s001_5m,s002=metrics_s002_5m" 配置
    //   为空时退化为单表模式(全部写入 srTable)
    public Map<String, String> sysidTableMap = new HashMap<>();
    // 未知 sysid 的兜底表;为空则丢弃未知 sysid 的数据
    public String defaultTable = "";

    // DLQ
    // dlqBootstrapServers 默认空:未显式指定时回落到 kafkaBrokers(本架构 DLQ 与
    // 消费集群同为本城 Kafka)。此前硬编码 localhost:9092 且无 CLI 参数可覆盖,
    // 生产环境任何一批写入失败走 DLQ 时都会连不上 broker → 阻塞 60s → 作业重启循环。
    public String dlqBootstrapServers = "";
    public String dlqTopic = "prom.local.dlq.sr.5m";
    public boolean dlqEnabled = true;

    // Flink
    public int windowMinutes = 5;
    public long checkpointIntervalMs = 60_000L;
    public String checkpointPath = "file:///tmp/flink-checkpoints";
    public long allowedLatenessMs = 30_000L;       // 30s
    // watermark 空闲检测:上游 subtask 超过该时长无数据则标记 idle,
    // 不再阻塞全局 watermark 推进(防止空闲 subtask 卡死 5min 窗口)
    public long watermarkIdlenessMs = 60_000L;     // 60s
    public int aggParallelism = 4;

    /** fromArgs 从命令行参数解析配置,支持 --env local/prod 预设。 */
    public static JobConfig fromArgs(String[] args) {
        JobConfig cfg = new JobConfig();
        for (int i = 0; i < args.length; i++) {
            String a = args[i];
            switch (a) {
                case "--env":
                    cfg.env = next(args, ++i);
                    cfg.applyEnvPreset();
                    break;
                case "--kafka-brokers":
                    cfg.kafkaBrokers = next(args, ++i);
                    break;
                case "--topic":
                    cfg.kafkaTopic = next(args, ++i);
                    break;
                case "--topics":
                    // 多 topic:逗号分隔,如 "prom.bj.routed.s001,prom.bj.routed.s002"
                    String topicsStr = next(args, ++i);
                    if (topicsStr != null && !topicsStr.isEmpty()) {
                        String[] parts = topicsStr.split(",");
                        java.util.List<String> list = new java.util.ArrayList<>();
                        for (String p : parts) {
                            String t = p.trim();
                            if (!t.isEmpty()) list.add(t);
                        }
                        if (!list.isEmpty()) {
                            cfg.kafkaTopics = list.toArray(new String[0]);
                        }
                    }
                    break;
                case "--group-id":
                    cfg.kafkaGroupId = next(args, ++i);
                    break;
                case "--starrocks-host":
                    cfg.srHost = next(args, ++i);
                    break;
                case "--starrocks-port":
                    cfg.srPort = Integer.parseInt(next(args, ++i));
                    break;
                case "--starrocks-db":
                    cfg.srDb = next(args, ++i);
                    break;
                case "--starrocks-table":
                    cfg.srTable = next(args, ++i);
                    break;
                case "--starrocks-user":
                    cfg.srUser = next(args, ++i);
                    break;
                case "--starrocks-password":
                    cfg.srPassword = next(args, ++i);
                    break;
                case "--label-prefix":
                    cfg.srLabelPrefix = next(args, ++i);
                    break;
                case "--sr-batch-size":
                    cfg.srBatchSize = Integer.parseInt(next(args, ++i));
                    break;
                case "--sr-batch-interval-ms":
                    cfg.srBatchIntervalMs = Long.parseLong(next(args, ++i));
                    break;
                case "--sysid-table-mapping":
                    // sysid → 表名 映射,格式: "s001=metrics_s001_5m,s002=metrics_s002_5m"
                    String mappingStr = next(args, ++i);
                    if (mappingStr != null && !mappingStr.isEmpty()) {
                        for (String pair : mappingStr.split(",")) {
                            String[] kv = pair.trim().split("=", 2);
                            if (kv.length == 2 && !kv[0].trim().isEmpty() && !kv[1].trim().isEmpty()) {
                                cfg.sysidTableMap.put(kv[0].trim(), kv[1].trim());
                            }
                        }
                    }
                    break;
                case "--default-table":
                    cfg.defaultTable = next(args, ++i);
                    break;
                case "--dlq-bootstrap-servers":
                    cfg.dlqBootstrapServers = next(args, ++i);
                    break;
                case "--dlq-topic":
                    cfg.dlqTopic = next(args, ++i);
                    break;
                case "--dlq-enabled":
                    cfg.dlqEnabled = Boolean.parseBoolean(next(args, ++i));
                    break;
                case "--source-parallelism":
                    cfg.sourceParallelism = Integer.parseInt(next(args, ++i));
                    break;
                case "--agg-parallelism":
                    cfg.aggParallelism = Integer.parseInt(next(args, ++i));
                    break;
                case "--checkpoint-path":
                    cfg.checkpointPath = next(args, ++i);
                    break;
                case "--checkpoint-interval-ms":
                    cfg.checkpointIntervalMs = Long.parseLong(next(args, ++i));
                    break;
                case "--window-minutes":
                    cfg.windowMinutes = Integer.parseInt(next(args, ++i));
                    break;
                case "--allowed-lateness-ms":
                    cfg.allowedLatenessMs = Long.parseLong(next(args, ++i));
                    break;
                case "--watermark-idleness-ms":
                    cfg.watermarkIdlenessMs = Long.parseLong(next(args, ++i));
                    break;
                case "--kafka-start-from":
                    cfg.kafkaStartFrom = next(args, ++i);
                    break;
                case "--kafka-offset-reset":
                    cfg.kafkaOffsetReset = next(args, ++i);
                    break;
                case "--kafka-start-timestamp":
                    cfg.kafkaStartTimestamp = Long.parseLong(next(args, ++i));
                    break;
                default:
                    // 忽略未知参数
            }
        }
        // DLQ broker 未显式指定时回落到消费集群(本架构 DLQ 与消费集群同为本城 Kafka)
        if (cfg.dlqBootstrapServers == null || cfg.dlqBootstrapServers.isEmpty()) {
            cfg.dlqBootstrapServers = cfg.kafkaBrokers;
        }
        return cfg;
    }

    /**
     * getKafkaTopics 返回实际消费的 topic 列表。
     * 若配置了 --topics 则返回多 topic 数组;否则返回单元素数组 [kafkaTopic]。
     */
    public String[] getKafkaTopics() {
        if (kafkaTopics != null && kafkaTopics.length > 0) {
            return kafkaTopics;
        }
        return new String[]{kafkaTopic};
    }

    /** applyEnvPreset 按 env 设置默认值。 */
    private void applyEnvPreset() {
        if ("prod".equals(env)) {
            kafkaSecurityProtocol = "SASL_SSL";
            kafkaSaslMechanism = "SCRAM-SHA-512";
            srGzip = true;
            checkpointPath = "hdfs:///flink/checkpoints/agg5m";
        }
    }

    private static String next(String[] args, int i) {
        return (i < args.length) ? args[i] : "";
    }

    @Override
    public String toString() {
        return "JobConfig{env='" + env + "', kafkaBrokers='" + kafkaBrokers
                + "', topics=" + java.util.Arrays.toString(getKafkaTopics())
                + ", srHost='" + srHost + ":" + srPort
                + "', srTable='" + srDb + "." + srTable
                + "', sysidTableMap=" + sysidTableMap
                + ", defaultTable='" + defaultTable
                + "', srBatchSize=" + srBatchSize
                + ", srBatchIntervalMs=" + srBatchIntervalMs
                + "', dlqTopic='" + dlqTopic
                + "', windowMinutes=" + windowMinutes
                + ", kafkaStartFrom='" + kafkaStartFrom
                + "', kafkaOffsetReset='" + kafkaOffsetReset
                + "', kafkaStartTimestamp=" + kafkaStartTimestamp + "}";
    }
}

