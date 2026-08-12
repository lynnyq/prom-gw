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
import com.example.promgw.sink.StarRocksSink;
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
 *     → StarRocksSink(Stream Load 写入北京 StarRocks)
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
        DataStream<KafkaRecord> kafkaStream = env.fromSource(
                kafkaSource,
                WatermarkStrategy.noWatermarks(),
                "kafka-source"
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
        DataStream<SampleWithMeta> withWatermark = samples
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<SampleWithMeta>forBoundedOutOfOrderness(Duration.ofMillis(cfg.allowedLatenessMs))
                                .withTimestampAssigner((rec, ts) -> rec.getTimestampMs())
                );

        // 7. keyBy(seriesKey) + 5min 窗口聚合
        DataStream<AggResult> aggStream = withWatermark
                .keyBy(rec -> LabelsHasher.seriesKey(rec.getTenant(), rec.getMetric(), rec.getLabels()))
                .window(TumblingEventTimeWindows.of(Time.minutes(cfg.windowMinutes)))
                .aggregate(new MetricAggFunction(), new AggWindowFunction())
                .name("agg-" + cfg.windowMinutes + "min")
                .setParallelism(cfg.aggParallelism);

        // 8. Stream Load 写入 StarRocks
        DlqHandler dlqHandler = cfg.dlqEnabled
                ? new KafkaDlqHandler(cfg.dlqBootstrapServers, cfg.dlqTopic)
                : null;

        aggStream.addSink(new StarRocksSink(
                cfg.srHost, cfg.srPort, cfg.srDb, cfg.srTable,
                cfg.srUser, cfg.srPassword, cfg.srGzip,
                cfg.srLabelPrefix, dlqHandler
        )).name("starrocks-stream-load");

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
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                .setDeserializer(new KafkaRecordDeserializer())
                .setProperties(props)
                .build();
    }
}
