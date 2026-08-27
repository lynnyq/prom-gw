package com.example.promgw;

import com.example.promgw.aggregate.AggResult;
import com.example.promgw.aggregate.AggWindowFunction;
import com.example.promgw.aggregate.ExpandWriteRequest;
import com.example.promgw.aggregate.MetricAggFunction;
import com.example.promgw.aggregate.SampleWithMeta;
import com.example.promgw.decoder.DedupFunction;
import com.example.promgw.decoder.KafkaRecord;
import com.example.promgw.decoder.KafkaRecordDeserializer;
import com.example.promgw.decoder.PromSample;
import com.example.promgw.decoder.PromWriteRequestDecoder;
import com.example.promgw.dlq.KafkaDlqHandler;
import com.example.promgw.sink.DlqHandler;
import com.example.promgw.sink.BufferingStarRocksSink;
import com.example.promgw.util.LabelsHasher;
import java.time.Duration;
import java.util.Properties;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Agg5mJob 主作业入口。
 *
 * 数据流:
 *   Kafka(prom-gw 写入的 snappy+protobuf)
 *     → KafkaRecordDeserializer(保留 key+headers)
 *     → DedupFunction(按 payload hash 去重,解决 prom-gw "N 条同 payload 消息"问题)
 *     → ExpandWriteRequest(展开 WriteRequest → 单条 sample)
 *     → watermark(基于 prompb.Sample.timestamp,允许 30s 乱序)
 *     → keyBy(seriesKey) + 5min tumbling window
 *     → MetricAggFunction(sum/count/max/min/p50/p99)
 *     → AggWindowFunction(组装 AggResult)
 *     → BufferingStarRocksSink(攒批 Stream Load 写入北京 StarRocks)
 *
 * 用法:
 *   本地: java -jar flink-agg5m-starrocks-1.0.0.jar --env local
 *   生产: flink run -d -p 24 -c com.example.promgw.Agg5mJob flink-agg5m-starrocks-1.0.0.jar --env prod \
 *           --kafka-brokers kafka-1.sz:9094,... --topic prom.sz.routed.app_business \
 *           --starrocks-host <beijing-fe-vip> --label-prefix sz_5m
 */
public class Agg5mJob {

    private static final Logger LOG = LoggerFactory.getLogger(Agg5mJob.class);

    public static void main(String[] args) throws Exception {
        JobConfig cfg = JobConfig.fromArgs(args);
        LOG.info("starting Agg5mJob with config: {}", cfg);

        // 1. 执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(cfg.checkpointIntervalMs, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30_000L);
        env.getCheckpointConfig().setCheckpointTimeout(300_000L);
        env.getCheckpointConfig().setExternalizedCheckpointCleanup(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointStorage(cfg.checkpointPath);

        // 2. Kafka Source
        KafkaSource<KafkaRecord> kafkaSource = buildKafkaSource(cfg);

        // 3. 构建数据流
        //    显式传入 TypeInformation:Flink 对 KafkaSource 泛型的自动推导可能落到
        //    Kryo,而 Kryo 在 JDK 17+(未加 --add-opens)初始化即失败。
        //    KafkaRecord.typeInfo() 全部使用 Flink 原生序列化器,不触发 Kryo。
        DataStream<KafkaRecord> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source",
                KafkaRecord.typeInfo()
        ).setParallelism(cfg.sourceParallelism);

        // 4. 按 payload hash 去重 + 解码
        //    prom-gw 一个 WriteRequest 产生 N 条同 payload 消息,用 payload hash 作 key
        //    让它们落同 subtask,DedupFunction 内部用状态去重
        DataStream<PromSample> deduped = kafkaStream
                .keyBy(r -> PromWriteRequestDecoder.payloadHash(r.getValue()))
                .process(new DedupFunction())
                .name("dedup-and-decode");

        // 5. 展开 WriteRequest → 单条 sample
        DataStream<SampleWithMeta> samples = deduped
                .flatMap(new ExpandWriteRequest())
                .name("expand-samples");

        // 6. 分配 watermark(事件时间 = prompb.Sample.timestamp,允许 30s 乱序)
        //    withIdleness(关键):watermark 在 keyBy(payloadHash) 去重重分区之后分配,
        //    若某个 subtask 分不到数据(hash 倾斜/dedup 丢弃重复消息后无输出),
        //    下游窗口算子的 watermark = 所有上游 channel 的最小值 → 卡死 →
        //    5min 事件时间窗口永不触发 → sink 一条数据都收不到。
        //    withIdleness 让空闲 subtask 在超时后不再阻塞全局 watermark 推进。
        DataStream<SampleWithMeta> withWatermark = samples
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SampleWithMeta>forBoundedOutOfOrderness(Duration.ofMillis(cfg.allowedLatenessMs))
                                .withTimestampAssigner((rec, ts) -> rec.getTimestampMs())
                                .withIdleness(Duration.ofMillis(cfg.watermarkIdlenessMs))
                );

        // 7. keyBy(seriesKey) + 5min 窗口聚合
        DataStream<AggResult> aggStream = withWatermark
                .keyBy(rec -> LabelsHasher.seriesKey(rec.getBusiness(), rec.getMetric(), rec.getLabels()))
                .window(TumblingEventTimeWindows.of(Time.minutes(cfg.windowMinutes)))
                .aggregate(new MetricAggFunction(), new AggWindowFunction())
                .name("agg-" + cfg.windowMinutes + "min")
                .setParallelism(cfg.aggParallelism);

        // 8. Stream Load 写入 StarRocks
        DlqHandler dlqHandler = cfg.dlqEnabled
                ? new KafkaDlqHandler(cfg.dlqBootstrapServers, cfg.dlqTopic)
                : null;

        aggStream.addSink(new BufferingStarRocksSink(
                cfg.srHost, cfg.srPort, cfg.srDb, cfg.srTable,
                cfg.srUser, cfg.srPassword, cfg.srGzip,
                cfg.srLabelPrefix, dlqHandler,
                cfg.srBatchSize, cfg.srBatchIntervalMs
        )).name("starrocks-stream-load-batch");

        // 9. 执行
        env.execute("flink-agg5m-" + cfg.env);
    }

    /** buildKafkaSource 构造 KafkaSource,含 SASL/SSL 配置。 */
    private static KafkaSource<KafkaRecord> buildKafkaSource(JobConfig cfg) {
        Properties props = new Properties();
        if (!cfg.kafkaSecurityProtocol.isEmpty()) {
            props.setProperty("security.protocol", cfg.kafkaSecurityProtocol);
            if (!cfg.kafkaSaslMechanism.isEmpty()) {
                props.setProperty("sasl.mechanism", cfg.kafkaSaslMechanism);
                props.setProperty("sasl.jaas.config", cfg.kafkaSaslJaasConfig);
            }
            if (!cfg.kafkaSslTruststoreLocation.isEmpty()) {
                props.setProperty("ssl.truststore.location", cfg.kafkaSslTruststoreLocation);
                props.setProperty("ssl.truststore.password", cfg.kafkaSslTruststorePassword);
            }
        }

        return KafkaSource.<KafkaRecord>builder()
                .setBootstrapServers(cfg.kafkaBrokers)
                .setTopics(cfg.kafkaTopic)
                .setGroupId(cfg.kafkaGroupId)
                .setStartingOffsets(buildOffsetsInitializer(cfg))
                .setDeserializer(new KafkaRecordDeserializer())
                .setProperties(props)
                .build();
    }

    /**
     * buildOffsetsInitializer 根据配置构造 Kafka 起始位点策略。
     *
     * 支持的 kafkaStartFrom 取值:
     *   committed  - 从已提交位点消费,无位点时按 kafkaOffsetReset 重置(默认)
     *   earliest   - 从最早位点消费
     *   latest     - 从最新位点消费
     *   timestamp  - 从 kafkaStartTimestamp(epoch millis)之后的首个位点消费
     */
    static OffsetsInitializer buildOffsetsInitializer(JobConfig cfg) {
        String startFrom = cfg.kafkaStartFrom == null ? "" : cfg.kafkaStartFrom.trim().toLowerCase();
        switch (startFrom) {
            case "earliest":
                return OffsetsInitializer.earliest();
            case "latest":
                return OffsetsInitializer.latest();
            case "timestamp":
                if (cfg.kafkaStartTimestamp <= 0L) {
                    throw new IllegalArgumentException(
                            "--kafka-start-timestamp 必须为正数(epoch millis) when --kafka-start-from=timestamp");
                }
                return OffsetsInitializer.timestamp(cfg.kafkaStartTimestamp);
            case "committed":
            case "":
                return OffsetsInitializer.committedOffsets(parseOffsetResetStrategy(cfg.kafkaOffsetReset));
            default:
                throw new IllegalArgumentException(
                        "不支持的 --kafka-start-from 取值: " + cfg.kafkaStartFrom
                                + " (可选: committed/earliest/latest/timestamp)");
        }
    }

    /** parseOffsetResetStrategy 解析无已提交位点时的重置策略。 */
    private static OffsetResetStrategy parseOffsetResetStrategy(String reset) {
        String r = reset == null ? "" : reset.trim().toLowerCase();
        switch (r) {
            case "earliest":
                return OffsetResetStrategy.EARLIEST;
            case "none":
                return OffsetResetStrategy.NONE;
            case "latest":
            case "":
                return OffsetResetStrategy.LATEST;
            default:
                throw new IllegalArgumentException(
                        "不支持的 --kafka-offset-reset 取值: " + reset
                                + " (可选: earliest/latest/none)");
        }
    }
}