package com.example.promgw.decoder;

import java.io.Serializable;
import java.util.Map;

/**
 * KafkaRecord 封装 Kafka 一条消息的完整信息。
 *
 * prom-gw 写入 Kafka 的消息:
 *   key    = SeriesKey 十进制字符串(uint64 FNV-1a hash,不可反解)
 *   value  = snappy(prompb.WriteRequest protobuf)  (Kafka 端 zstd 由 connector 自动解)
 *   headers: tenant / source_dc / ingest_city / ingest_dc / ingest_time_ms / traceparent
 *
 * 为什么不用 Flink 内置的 ConsumerRecord?
 *   - 需要在算子间传递完整的 key+value+headers,内置类型不便于 POJO 序列化
 *   - 显式 POJO 便于单元测试
 *
 * 实现 {@link Serializable}:作为 DataStream 中的传输对象,需支持 Flink 序列化。
 */
public class KafkaRecord implements Serializable {

    private static final long serialVersionUID = 1L;
    private byte[] key;
    private byte[] value;
    private String topic;
    private long offset;
    private int partition;
    private long timestamp;
    private Map<String, String> headers;

    // Flink POJO 需要无参构造
    public KafkaRecord() {}

    public KafkaRecord(byte[] key, byte[] value, String topic, long offset,
                       int partition, long timestamp, Map<String, String> headers) {
        this.key = key;
        this.value = value;
        this.topic = topic;
        this.offset = offset;
        this.partition = partition;
        this.timestamp = timestamp;
        this.headers = headers;
    }

    public byte[] getKey() { return key; }
    public void setKey(byte[] key) { this.key = key; }

    public byte[] getValue() { return value; }
    public void setValue(byte[] value) { this.value = value; }

    public String getTopic() { return topic; }
    public void setTopic(String topic) { this.topic = topic; }

    public long getOffset() { return offset; }
    public void setOffset(long offset) { this.offset = offset; }

    public int getPartition() { return partition; }
    public void setPartition(int partition) { this.partition = partition; }

    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }

    public Map<String, String> getHeaders() { return headers; }
    public void setHeaders(Map<String, String> headers) { this.headers = headers; }

    @Override
    public String toString() {
        return "KafkaRecord{topic='" + topic + "', partition=" + partition
                + ", offset=" + offset + ", keyLen=" + (key == null ? 0 : key.length)
                + ", valueLen=" + (value == null ? 0 : value.length) + "}";
    }
}
