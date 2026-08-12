package com.example.promgw.decoder;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.promgw.proto.PromProtos.Label;
import com.example.promgw.proto.PromProtos.Sample;
import com.example.promgw.proto.PromProtos.TimeSeries;
import com.example.promgw.proto.PromProtos.WriteRequest;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.xerial.snappy.Snappy;

/**
 * PromWriteRequestDecoderTest 验证 snappy + protobuf 解码正确性。
 *
 * 构造一个 WriteRequest → snappy 编码 → 用 PromWriteRequestDecoder 解码 → 断言字段。
 */
class PromWriteRequestDecoderTest {

    private final PromWriteRequestDecoder decoder = new PromWriteRequestDecoder();

    @Test
    void testDecodeSingleSample() throws IOException {
        // 构造 WriteRequest:1 个 series,1 个 sample
        byte[] protobufBytes = WriteRequest.newBuilder()
                .addTimeseries(TimeSeries.newBuilder()
                        .addLabels(Label.newBuilder().setName("__name__").setValue("up").build())
                        .addLabels(Label.newBuilder().setName("job").setValue("prometheus").build())
                        .addLabels(Label.newBuilder().setName("instance").setValue("localhost:9090").build())
                        .addSamples(Sample.newBuilder().setValue(1.0).setTimestamp(1786431389000L).build())
                        .build())
                .build().toByteArray();
        byte[] snappyBytes = Snappy.compress(protobufBytes);

        // 模拟 Kafka headers(prom-gw 写入的元数据)
        Map<String, String> headers = new HashMap<>();
        headers.put("tenant", "app-business");
        headers.put("source_dc", "dc-bj-dongba");
        headers.put("ingest_city", "bj");
        headers.put("ingest_dc", "dc-bj-dongba");
        headers.put("ingest_time_ms", "1786431389413");
        headers.put("traceparent", "00-abc-def-01");

        PromSample sample = decoder.decode(snappyBytes, headers);

        assertThat(sample).isNotNull();
        assertThat(sample.getTenant()).isEqualTo("app-business");
        assertThat(sample.getSourceDc()).isEqualTo("dc-bj-dongba");
        assertThat(sample.getIngestCity()).isEqualTo("bj");
        assertThat(sample.getIngestTimeMs()).isEqualTo(1786431389413L);
        assertThat(sample.getTraceparent()).isEqualTo("00-abc-def-01");

        // 验证 series
        assertThat(sample.getTimeseries()).hasSize(1);
        PromSample.ParsedSeries series = sample.getTimeseries().get(0);
        assertThat(series.getMetricName()).isEqualTo("up");
        assertThat(series.getLabels())
                .containsEntry("job", "prometheus")
                .containsEntry("instance", "localhost:9090")
                // __name__ 已拆出存到 metricName,不在 labels 里
                .doesNotContainKey("__name__");

        // 验证 sample
        assertThat(series.getSamples()).hasSize(1);
        PromSample.ParsedSample s = series.getSamples().get(0);
        assertThat(s.getValue()).isEqualTo(1.0);
        assertThat(s.getTimestampMs()).isEqualTo(1786431389000L);
    }

    @Test
    void testDecodeMultipleSeries() throws IOException {
        // 构造 WriteRequest:2 个 series,各 1 个 sample
        byte[] protobufBytes = WriteRequest.newBuilder()
                .addTimeseries(TimeSeries.newBuilder()
                        .addLabels(Label.newBuilder().setName("__name__").setValue("http_requests_total").build())
                        .addLabels(Label.newBuilder().setName("method").setValue("GET").build())
                        .addSamples(Sample.newBuilder().setValue(100.0).setTimestamp(1786431390000L).build())
                        .build())
                .addTimeseries(TimeSeries.newBuilder()
                        .addLabels(Label.newBuilder().setName("__name__").setValue("http_requests_total").build())
                        .addLabels(Label.newBuilder().setName("method").setValue("POST").build())
                        .addSamples(Sample.newBuilder().setValue(50.0).setTimestamp(1786431390000L).build())
                        .build())
                .build().toByteArray();
        byte[] snappyBytes = Snappy.compress(protobufBytes);

        Map<String, String> headers = new HashMap<>();
        headers.put("tenant", "infra");
        headers.put("ingest_city", "sz");

        PromSample sample = decoder.decode(snappyBytes, headers);

        assertThat(sample).isNotNull();
        assertThat(sample.getTenant()).isEqualTo("infra");
        assertThat(sample.getIngestCity()).isEqualTo("sz");
        assertThat(sample.getTimeseries()).hasSize(2);

        // 第 1 个 series
        PromSample.ParsedSeries s1 = sample.getTimeseries().get(0);
        assertThat(s1.getMetricName()).isEqualTo("http_requests_total");
        assertThat(s1.getLabels()).containsEntry("method", "GET");
        assertThat(s1.getSamples().get(0).getValue()).isEqualTo(100.0);

        // 第 2 个 series
        PromSample.ParsedSeries s2 = sample.getTimeseries().get(1);
        assertThat(s2.getLabels()).containsEntry("method", "POST");
        assertThat(s2.getSamples().get(0).getValue()).isEqualTo(50.0);
    }

    @Test
    void testDecodeEmptyValue() throws IOException {
        PromSample sample = decoder.decode(new byte[0], new HashMap<>());
        assertThat(sample).isNull();
    }

    @Test
    void testDecodeNullValue() throws IOException {
        PromSample sample = decoder.decode(null, new HashMap<>());
        assertThat(sample).isNull();
    }

    @Test
    void testPayloadHashConsistency() {
        byte[] payload1 = "hello-world".getBytes();
        byte[] payload2 = "hello-world".getBytes();
        byte[] payload3 = "different".getBytes();

        // 相同内容 hash 相同
        assertThat(PromWriteRequestDecoder.payloadHash(payload1))
                .isEqualTo(PromWriteRequestDecoder.payloadHash(payload2));
        // 不同内容 hash 不同
        assertThat(PromWriteRequestDecoder.payloadHash(payload1))
                .isNotEqualTo(PromWriteRequestDecoder.payloadHash(payload3));
    }

    @Test
    void testPayloadHashNull() {
        assertThat(PromWriteRequestDecoder.payloadHash(null)).isZero();
    }
}
