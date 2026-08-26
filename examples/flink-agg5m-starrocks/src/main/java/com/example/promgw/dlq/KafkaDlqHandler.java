package com.example.promgw.dlq;

import com.example.promgw.sink.DlqHandler;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * KafkaDlqHandler 把失败的 Stream Load 批次写回本城 Kafka DLQ topic。
 *
 * DLQ topic 命名:prom.<city>.dlq.sr.5m
 * 由运维工具(C 作业)定期消费并重新写 StarRocks,超过 max_retry 则告警。
 *
 * 序列化说明:本类作为 StarRocksSink 的字段被 Flink ClosureCleaner 序列化后
 * 分发到 TaskManager。bootstrapServers/dlqTopic 为可序列化基本类型;
 * mapper/producer 为运行时资源,标记 transient 在 open() 中重建。
 */
public class KafkaDlqHandler implements DlqHandler, Serializable {

    private static final long serialVersionUID = 1L;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaDlqHandler.class);

    private final String bootstrapServers;
    private final String dlqTopic;
    private transient ObjectMapper mapper;

    private transient KafkaProducer<byte[], byte[]> producer;

    public KafkaDlqHandler(String bootstrapServers, String dlqTopic) {
        this.bootstrapServers = bootstrapServers;
        this.dlqTopic = dlqTopic;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void open() {
        // mapper 可能在反序列化后为 null(TaskManager 侧),这里重建
        if (mapper == null) {
            mapper = new ObjectMapper();
        }
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("enable.idempotence", "true");
        // DLQ 是兜底通道,故障要快速暴露:默认 max.block.ms=60s 会让每次失败的
        // 发送阻塞 1 分钟(broker 不可达/topic 不存在时),拖垮 sink 吞吐。
        props.put("max.block.ms", 10_000);
        props.put("delivery.timeout.ms", 15_000);
        props.put("request.timeout.ms", 5_000);
        this.producer = new KafkaProducer<>(props);
        LOG.info("KafkaDlqHandler opened: brokers={}, topic={}", bootstrapServers, dlqTopic);
    }

    @Override
    public void send(String label, String payload, String error) throws Exception {
        DlqMessage msg = new DlqMessage(payload, label, error, 0, System.currentTimeMillis());
        byte[] value = mapper.writeValueAsBytes(msg);
        // 用 label 作 key,保证同 label 重试落同 partition
        ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                dlqTopic, label.getBytes(StandardCharsets.UTF_8), value);
        // 同步发送:等待 broker ack 后再返回。
        // 修复前使用异步 callback,send() 立即返回 → StarRocksSink.invoke 认为 DLQ 成功 →
        // checkpoint 推进 → Kafka ack 实际失败 → 数据静默丢失(只记 LOG.error)。
        // 修复后 .get() 阻塞等待 ack,失败抛 Exception → 由 StarRocksSink.invoke 传播到 Flink →
        // 触发 task 从 last checkpoint 重启,保证 at-least-once 语义。
        try {
            producer.send(record).get();
            LOG.warn("DLQ sent: label={}, topic={}", label, dlqTopic);
        } catch (Exception e) {
            LOG.error("DLQ send failed, propagating to ensure at-least-once: label={}", label, e);
            throw e;
        }
    }

    @Override
    public void close() {
        if (producer != null) {
            producer.flush();
            producer.close();
        }
    }

    // ---- 以下为测试辅助方法(package-private) ----

    String getBootstrapServers() {
        return bootstrapServers;
    }

    String getDlqTopic() {
        return dlqTopic;
    }

    boolean isMapperNull() {
        return mapper == null;
    }

    boolean isProducerNull() {
        return producer == null;
    }

    /** recreateMapperIfNeeded 在 mapper 为 null 时重建,返回是否执行了重建。 */
    boolean recreateMapperIfNeeded() {
        if (mapper == null) {
            mapper = new ObjectMapper();
            return true;
        }
        return false;
    }
}
