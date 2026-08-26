package com.example.promgw.decoder;

import com.example.promgw.proto.PromProtos.WriteRequest;
import java.io.IOException;
import java.io.Serializable;
import java.util.Map;
import org.xerial.snappy.Snappy;

/**
 * PromWriteRequestDecoder 解码 prom-gw 写入 Kafka 的消息。
 *
 * 编码层次:
 *   Kafka 端 zstd(由 Flink KafkaSource connector 自动解)
 *     → snappy 压缩字节(本类负责解)
 *       → prompb.WriteRequest protobuf 字节(本类负责 unmarshal)
 *
 * 关键设计(prom-gw internal/ruleengine/pipeline.go:225-242):
 *   一个 WriteRequest 含 N 个 sample → 产生 N 条 Kafka 消息,payload 完全相同,Key 不同。
 *   因此本类不直接输出单条 sample,而是输出整个 WriteRequest + header 元数据,
 *   由上游 DedupFunction 按 payload hash 去重后再展开。
 *
 * 实现 {@link Serializable}:本类被 DedupFunction 作为字段持有,DedupFunction
 * 作为 Flink 算子会被 ClosureCleaner 序列化分发到 TaskManager。
 */
public class PromWriteRequestDecoder implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * decode 解码一条 Kafka 消息。
     *
     * @param value   snappy 压缩的 WriteRequest 字节(Kafka 端 zstd 已由 connector 自动解)
     * @param headers Kafka headers(含 tenant/source_dc/ingest_city/...)
     * @return PromSample POJO(含所有 timeseries + 元数据)
     */
    public PromSample decode(byte[] value, Map<String, String> headers) throws IOException {
        if (value == null || value.length == 0) {
            return null;
        }

        // 1. 解 snappy(内层压缩)
        byte[] protobufBytes = Snappy.uncompress(value);

        // 2. protobuf Unmarshal → WriteRequest,再提取 POJO
        return WriteRequestParser.parse(protobufBytes, headers);
    }

    /**
     * decodePayloadHash 计算 payload 的 hash,用于去重 key。
     *
     * 同一个 WriteRequest 产生的 N 条消息 payload 相同,hash 也相同,
     * 用此值作 keyBy 即可让它们落到同一 subtask,便于去重。
     */
    public static int payloadHash(byte[] value) {
        if (value == null) return 0;
        // Java Arrays.hashCode 足够,无需加密强度
        int h = 1;
        for (byte b : value) {
            h = 31 * h + b;
        }
        return h;
    }
}
