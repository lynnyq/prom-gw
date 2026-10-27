package com.lynnyq.promgw.dlq;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.lynnyq.promgw.sink.StarRocksStreamLoadClient;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * DlqReplayer DLQ 重放工具。
 *
 * 消费 Kafka DLQ topic(prom.&lt;city&gt;.dlq.sr.5m),把失败的聚合数据重新写入 StarRocks。
 *
 * 核心流程:
 *   1. 消费 DLQ 消息,反序列化为 {@link DlqMessage}
 *   2. 读取消息中的 table 字段,按表分桶(single table 模式下统一使用 fallback 表)
 *   3. 攒批(按表独立):buffer 满 batch.size 或全局超时 batch.wait.ms → flush
 *   4. flush:调用 StarRocks Stream Load,失败则指数退避重试,超过 maxRetry 告警并跳过
 *   5. 整个 poll 批次处理完后 commit offset(at-least-once)
 *
 * 表路由:
 *   - DLQ 消息的 table 字段由 Flink Sink 写入(BufferingStarRocksSink.sendRowsToDlq)
 *   - 若 table 为空,fallback 到配置 starrocks.table
 *   - 每个 table 对应一个独立的 StarRocksStreamLoadClient 实例(缓存复用)
 *
 * 幂等性:
 *   - 复用 DLQ 消息中的 label(Stream Load label),StarRocks 对同 label 做幂等去重
 *   - 因此重放工具重放不会产生重复数据
 *
 * 运行: java -cp flink-agg5m-starrocks-1.0.0.jar com.lynnyq.promgw.dlq.DlqReplayer /path/to/dlq-replayer.properties
 */
public class DlqReplayer implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(DlqReplayer.class);

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final DlqReplayerConfig config;
    private final Map<String, StarRocksStreamLoadClient> clientCache = new HashMap<>();
    private final Map<String, List<DlqMessage>> bufferMap = new HashMap<>();
    private volatile boolean running = true;

    public DlqReplayer(DlqReplayerConfig config) {
        this.config = config;
    }

    /**
     * run 启动消费主循环,直到 {@link #stop()} 被调用。
     *
     * 处理语义:at-least-once。每个 poll 批次的所有消息处理完成(成功或毒丸跳过)
     * 后才 commit offset。进程崩溃时未 commit 的消息会被重新消费,StarRocks 幂等去重保证不重复。
     */
    public void run() {
        LOG.info("DlqReplayer starting: topic={}, group={}, srHost={}, db={}, fallbackTable={}",
                config.getKafkaDlqTopic(), config.getKafkaConsumerGroupId(),
                config.getSrFeHost(), config.getSrDb(), config.getSrTableFallback());

        try (KafkaConsumer<byte[], byte[]> consumer = createConsumer()) {
            consumer.subscribe(List.of(config.getKafkaDlqTopic()));
            LOG.info("DlqReplayer subscribed to topic: {}", config.getKafkaDlqTopic());

            while (running) {
                ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(5000));
                if (records.isEmpty()) {
                    // poll 空也要 flush 超时 buffer,避免小批量数据长时间滞留
                    flushAllBuffersIfTimeout();
                    continue;
                }

                LOG.debug("polled {} records from DLQ", records.count());
                for (ConsumerRecord<byte[], byte[]> record : records) {
                    handleRecord(record);
                }

                // flush 所有达到 batch.size 的 buffer
                flushFullBuffers();

                // 整个 poll 批次处理完后 commit offset
                consumer.commitSync();
            }
        } catch (Exception e) {
            LOG.error("DlqReplayer fatal error, exiting", e);
            throw new RuntimeException(e);
        } finally {
            // 退出前 flush 剩余 buffer
            flushAllBuffers();
            closeClients();
            LOG.info("DlqReplayer stopped");
        }
    }

    /** handleRecord 处理单条 Kafka 消息:反序列化 → 按表分桶。 */
    private void handleRecord(ConsumerRecord<byte[], byte[]> record) {
        DlqMessage msg;
        try {
            msg = MAPPER.readValue(record.value(), DlqMessage.class);
        } catch (Exception e) {
            // 反序列化失败的消息直接跳过(毒丸),避免阻塞整个队列
            LOG.error("failed to deserialize DLQ message at offset={}, skipping (poison pill)",
                    record.offset(), e);
            return;
        }

        String table = resolveTable(msg);
        if (table == null || table.isEmpty()) {
            LOG.error("DLQ message has no table and no fallback configured, skipping: label={}",
                    msg.getLabel());
            return;
        }

        bufferMap.computeIfAbsent(table, k -> new ArrayList<>()).add(msg);
    }

    /**
     * resolveTable 解析目标 StarRocks 表名。
     *
     * 优先使用 DLQ 消息中的 table 字段(sysid 分表模式下由 Sink 写入);
     * 为空时 fallback 到配置 starrocks.table。
     *
     * @return 目标表名,不会为 null(除非 fallback 也未配置)
     */
    String resolveTable(DlqMessage msg) {
        String t = msg.getTable();
        if (t != null && !t.isEmpty()) {
            return t;
        }
        return config.getSrTableFallback();
    }

    /** flushFullBuffers flush 所有达到 batch.size 的表 buffer。 */
    private void flushFullBuffers() {
        for (Map.Entry<String, List<DlqMessage>> entry : bufferMap.entrySet()) {
            List<DlqMessage> buf = entry.getValue();
            if (buf.size() >= config.getBatchSize()) {
                flushBuffer(entry.getKey(), buf);
            }
        }
    }

    /** flushAllBuffersIfTimeout 距上次 flush 超过 batchWaitMs 时 flush 所有非空 buffer。 */
    private long lastFlushTime = System.currentTimeMillis();

    private void flushAllBuffersIfTimeout() {
        long now = System.currentTimeMillis();
        if (now - lastFlushTime >= config.getBatchWaitMs()) {
            flushAllBuffers();
        }
    }

    /** flushAllBuffers flush 所有非空 buffer(退出前或超时触发)。 */
    private void flushAllBuffers() {
        for (Map.Entry<String, List<DlqMessage>> entry : bufferMap.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                flushBuffer(entry.getKey(), entry.getValue());
            }
        }
        lastFlushTime = System.currentTimeMillis();
    }

    /**
     * flushBuffer 把指定表的 buffer 通过 Stream Load 写入 StarRocks。
     *
     * 失败处理:
     *   - 指数退避重试(backoff.base * multiplier^attempt,封顶 backoff.max)
     *   - 超过 maxRetry 后视为毒丸批次,告警并丢弃(不阻塞队列)
     *   - 成功后清空 buffer
     */
    void flushBuffer(String table, List<DlqMessage> buf) {
        StarRocksStreamLoadClient client = getOrCreateClient(table);
        flushBufferWithClient(table, buf, client);
    }

    /**
     * flushBufferWithClient 使用指定 client 执行 flush(供测试注入 stub)。
     *
     * @param table  目标表名(仅用于日志)
     * @param buf    待写入的消息列表(成功或丢弃后会被清空)
     * @param client Stream Load 客户端
     */
    void flushBufferWithClient(String table, List<DlqMessage> buf, StarRocksStreamLoadClient client) {
        if (buf.isEmpty()) {
            return;
        }
        // 用第一条消息的 label 作为 batch label(StarRocks 同 label 幂等)
        String label = buf.get(0).getLabel();
        String json = buildBatchJson(buf);

        for (int attempt = 0; attempt <= config.getReplayMaxRetry(); attempt++) {
            try {
                client.load(label, json, config.isSrGzip());
                LOG.info("DLQ replay success: table={}, label={}, rows={}", table, label, buf.size());
                buf.clear();
                return;
            } catch (Exception e) {
                if (attempt < config.getReplayMaxRetry()) {
                    long backoff = computeBackoffMs(attempt);
                    LOG.warn("DLQ replay failed (attempt {}/{}), retrying in {}ms: table={}, label={}, error={}",
                            attempt + 1, config.getReplayMaxRetry(), backoff, table, label, e.getMessage());
                    try {
                        Thread.sleep(backoff);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        LOG.error("replay interrupted during backoff", ie);
                        return;
                    }
                } else {
                    // 超过最大重试:毒丸批次,告警并丢弃
                    LOG.error("DLQ replay exhausted after {} retries, dropping {} rows (poison pill batch): "
                            + "table={}, label={}, error={}",
                            config.getReplayMaxRetry(), buf.size(), table, label, e.getMessage());
                    sendAlert(table, label, buf.size(), e.getMessage());
                    buf.clear();
                    return;
                }
            }
        }
    }

    /**
     * buildBatchJson 把多条 DLQ 消息的 original 字段拼成 JSON 数组。
     *
     * Stream Load strip_outer_array=true 需要数组格式:
     *   [ {original1}, {original2}, ... ]
     * original 字段已是单行 AggResult 的 JSON 字符串,直接拼接即可。
     */
    String buildBatchJson(List<DlqMessage> msgs) {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < msgs.size(); i++) {
            if (i > 0) {
                sb.append(",");
            }
            sb.append(msgs.get(i).getOriginal());
        }
        sb.append("]");
        return sb.toString();
    }

    /**
     * computeBackoffMs 计算指数退避时间。
     *
     * backoff = min(backoff.base * multiplier^attempt, backoff.max)
     */
    long computeBackoffMs(int attempt) {
        double backoff = config.getReplayBackoffBaseMs()
                * Math.pow(config.getReplayBackoffMultiplier(), attempt);
        return (long) Math.min(backoff, config.getReplayBackoffMaxMs());
    }

    /** getOrCreateClient 按表名获取或创建 StarRocksStreamLoadClient(缓存复用)。 */
    private StarRocksStreamLoadClient getOrCreateClient(String table) {
        return clientCache.computeIfAbsent(table, t -> new StarRocksStreamLoadClient(
                config.getSrFeHost(), config.getSrFePort(), config.getSrDb(), t,
                config.getSrUser(), config.getSrPassword(),
                config.getHttpConnectTimeoutMs(), config.getHttpSocketTimeoutMs()));
    }

    /** sendAlert 发送毒丸告警(当前仅记 ERROR 日志,可扩展为 webhook 调用)。 */
    private void sendAlert(String table, String label, int rowCount, String error) {
        if (!config.isAlertEnabled()) {
            return;
        }
        // TODO: 对接 AlertManager webhook(config.getAlertWebhook())
        LOG.error("[ALERT] DLQ poison pill batch: table={}, label={}, rows={}, error={}",
                table, label, rowCount, error);
    }

    /** createConsumer 创建 Kafka 消费者。 */
    private KafkaConsumer<byte[], byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getKafkaBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.getKafkaConsumerGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, config.getKafkaAutoOffsetReset());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, config.isKafkaEnableAutoCommit());
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, config.getKafkaMaxPollRecords());
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, config.getKafkaSessionTimeoutMs());
        // DLQ 重放不要求低延迟,适当延长 poll 间隔避免空转
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(props);
    }

    /** stop 优雅停止消费循环。 */
    public void stop() {
        running = false;
    }

    /** closeClients 关闭所有缓存的 StarRocks client。 */
    private void closeClients() {
        for (StarRocksStreamLoadClient client : clientCache.values()) {
            try {
                client.close();
            } catch (Exception e) {
                LOG.warn("failed to close StarRocks client", e);
            }
        }
        clientCache.clear();
    }

    @Override
    public void close() {
        closeClients();
    }

    // ====== 测试辅助方法(package-private) ======

    Map<String, List<DlqMessage>> getBufferMap() {
        return bufferMap;
    }

    Map<String, StarRocksStreamLoadClient> getClientCache() {
        return clientCache;
    }

    /**
     * main 入口。
     *
     * @param args args[0] = properties 配置文件路径
     */
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: java -cp flink-agg5m-starrocks-1.0.0.jar "
                    + "com.lynnyq.promgw.dlq.DlqReplayer <config.properties>");
            System.exit(1);
        }

        try {
            DlqReplayerConfig config = new DlqReplayerConfig(args[0]);
            DlqReplayer replayer = new DlqReplayer(config);

            // 注册 shutdown hook,支持 SIGTERM 优雅停止
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                LOG.info("shutdown signal received, stopping DlqReplayer...");
                replayer.stop();
            }));

            replayer.run();
        } catch (Exception e) {
            LOG.error("DlqReplayer failed to start", e);
            System.exit(1);
        }
    }
}
