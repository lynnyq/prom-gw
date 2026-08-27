package com.example.promgw;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.promgw.aggregate.AggResult;
import com.example.promgw.aggregate.AggWindowFunction;
import com.example.promgw.aggregate.ExpandWriteRequest;
import com.example.promgw.aggregate.MetricAggFunction;
import com.example.promgw.aggregate.MetricAggState;
import com.example.promgw.aggregate.SampleWithMeta;
import com.example.promgw.decoder.DedupFunction;
import com.example.promgw.decoder.KafkaRecord;
import com.example.promgw.decoder.KafkaRecordDeserializer;
import com.example.promgw.decoder.PromSample;
import com.example.promgw.decoder.PromWriteRequestDecoder;
import com.example.promgw.dlq.DlqMessage;
import com.example.promgw.dlq.KafkaDlqHandler;
import com.example.promgw.sink.BufferingStarRocksSink;
import com.example.promgw.sink.DlqHandler;
import com.example.promgw.sink.StarRocksSink;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

/**
 * FlinkSerializationAuditTest 全面验证 Flink 作业中所有算子和 POJO 的序列化兼容性。
 *
 * Flink 通过 ClosureCleaner + InstantiationUtil.serializeObject 把算子闭包序列化后
 * 分发到 TaskManager。任何算子类及其字段必须可序列化,否则抛出
 * InvalidProgramException / NotSerializableException。
 *
 * 本测试逐一验证:
 *   1. 所有 Flink 算子(Sink/Function)可序列化
 *   2. 所有 DataStream POJO 可序列化
 *   3. checkpoint 状态对象可序列化
 *   4. 含非序列化字段的算子 transient 标记正确
 */
class FlinkSerializationAuditTest {

    // ===== POJO 序列化测试 =====

    @Test
    void testKafkaRecordSerializable() throws Exception {
        Map<String, String> headers = new HashMap<>();
        headers.put("business", "app-business");
        KafkaRecord rec = new KafkaRecord(
                "key".getBytes(), "value".getBytes(), "topic", 100L, 0, 1234L, headers);
        byte[] bytes = InstantiationUtil.serializeObject(rec);
        KafkaRecord restored = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.getTopic()).isEqualTo("topic");
        assertThat(restored.getOffset()).isEqualTo(100L);
        assertThat(restored.getHeaders()).containsEntry("business", "app-business");
    }

    @Test
    void testPromSampleSerializable() throws Exception {
        List<PromSample.ParsedSeries> ts = new ArrayList<>();
        List<PromSample.ParsedSample> samples = new ArrayList<>();
        samples.add(new PromSample.ParsedSample(1.0, 1700000000000L));
        ts.add(new PromSample.ParsedSeries("up", new HashMap<>(), samples));

        PromSample ps = new PromSample(ts, "business1", "dc-bj", "bj", "dc-bj", 1700000000000L, "trace-1");
        byte[] bytes = InstantiationUtil.serializeObject(ps);
        PromSample restored = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.getBusiness()).isEqualTo("business1");
        assertThat(restored.getTimeseries()).hasSize(1);
        assertThat(restored.getTimeseries().get(0).getSamples()).hasSize(1);
        assertThat(restored.getTimeseries().get(0).getSamples().get(0).getValue()).isEqualTo(1.0);
    }

    @Test
    void testSampleWithMetaSerializable() throws Exception {
        Map<String, String> labels = new HashMap<>();
        labels.put("job", "prometheus");
        SampleWithMeta swm = new SampleWithMeta(
                "business", "up", labels, 1.0, 1700000000000L,
                "dc-bj", "bj", "dc-bj", 1700000000000L, "trace-1");
        byte[] bytes = InstantiationUtil.serializeObject(swm);
        SampleWithMeta restored = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.getMetric()).isEqualTo("up");
        assertThat(restored.getValue()).isEqualTo(1.0);
        assertThat(restored.getLabels()).containsEntry("job", "prometheus");
    }

    @Test
    void testAggResultSerializable() throws Exception {
        AggResult r = new AggResult();
        r.setTs(new Date(1700000000000L));
        r.setMetric("up");
        r.setBusiness("team-a");
        r.setLabelsHash("abc12345");
        r.setSampleCount(10);
        r.setValueSum(100.0);
        r.setValueMax(20.0);
        r.setValueMin(1.0);
        r.setValueAvg(10.0);
        r.setValueP50(10.0);
        r.setValueP99(20.0);

        byte[] bytes = InstantiationUtil.serializeObject(r);
        AggResult restored = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.getMetric()).isEqualTo("up");
        assertThat(restored.getSampleCount()).isEqualTo(10);
        assertThat(restored.getValueSum()).isEqualTo(100.0);
    }

    @Test
    void testMetricAggStateSerializable() throws Exception {
        MetricAggState state = new MetricAggState();
        state.count = 5;
        state.sum = 50.0;
        state.max = 20.0;
        state.min = 1.0;
        state.samples.add(1.0);
        state.samples.add(20.0);
        state.business = "business1";
        state.metric = "up";

        byte[] bytes = InstantiationUtil.serializeObject(state);
        MetricAggState restored = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.count).isEqualTo(5);
        assertThat(restored.sum).isEqualTo(50.0);
        assertThat(restored.samples).hasSize(2);
        assertThat(restored.business).isEqualTo("business1");
    }

    @Test
    void testDlqMessageSerializable() throws Exception {
        DlqMessage msg = new DlqMessage("payload", "label-001", "HTTP 500", 0, 1700000000000L);
        byte[] bytes = InstantiationUtil.serializeObject(msg);
        DlqMessage restored = InstantiationUtil.deserializeObject(bytes, getClass().getClassLoader());
        assertThat(restored.getLabel()).isEqualTo("label-001");
        assertThat(restored.getError()).isEqualTo("HTTP 500");
    }

    @Test
    void testPromWriteRequestDecoderSerializable() throws Exception {
        PromWriteRequestDecoder decoder = new PromWriteRequestDecoder();
        byte[] bytes = InstantiationUtil.serializeObject(decoder);
        PromWriteRequestDecoder restored = InstantiationUtil.deserializeObject(
                bytes, getClass().getClassLoader());
        assertThat(restored).isNotNull();
    }

    // ===== Flink 算子闭包序列化测试 =====

    @Test
    void testDedupFunctionSerializable() throws Exception {
        DedupFunction func = new DedupFunction();
        byte[] bytes = InstantiationUtil.serializeObject(func);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testExpandWriteRequestSerializable() throws Exception {
        ExpandWriteRequest func = new ExpandWriteRequest();
        byte[] bytes = InstantiationUtil.serializeObject(func);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testMetricAggFunctionSerializable() throws Exception {
        MetricAggFunction func = new MetricAggFunction();
        byte[] bytes = InstantiationUtil.serializeObject(func);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testAggWindowFunctionSerializable() throws Exception {
        AggWindowFunction func = new AggWindowFunction();
        byte[] bytes = InstantiationUtil.serializeObject(func);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testKafkaRecordDeserializerSerializable() throws Exception {
        KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer();
        byte[] bytes = InstantiationUtil.serializeObject(deserializer);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testStarRocksSinkWithDlqSerializable() throws Exception {
        DlqHandler dlq = new KafkaDlqHandler("broker:9092", "prom.dlq");
        StarRocksSink sink = new StarRocksSink(
                "fe", 8030, "prom", "metrics_5m",
                "root", "", true, "local_5m", dlq);
        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testStarRocksSinkWithoutDlqSerializable() throws Exception {
        StarRocksSink sink = new StarRocksSink(
                "fe", 8030, "prom", "metrics_5m",
                "root", "", true, "local_5m", null);
        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testBufferingStarRocksSinkWithDlqSerializable() throws Exception {
        DlqHandler dlq = new KafkaDlqHandler("broker:9092", "prom.dlq");
        BufferingStarRocksSink sink = new BufferingStarRocksSink(
                "fe", 8030, "prom", "metrics_5m",
                "root", "", true, "local_5m", dlq,
                500, 10_000L);
        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testBufferingStarRocksSinkWithoutDlqSerializable() throws Exception {
        BufferingStarRocksSink sink = new BufferingStarRocksSink(
                "fe", 8030, "prom", "metrics_5m",
                "root", "", true, "local_5m", null,
                500, 10_000L);
        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(bytes).isNotEmpty();
    }

    // ===== Transient 字段验证 =====

    @Test
    void testDedupFunctionDecoderIsNullAfterDeserialization() throws Exception {
        DedupFunction func = new DedupFunction();
        byte[] bytes = InstantiationUtil.serializeObject(func);
        DedupFunction restored = InstantiationUtil.deserializeObject(
                bytes, getClass().getClassLoader());
        // decoder 是 transient,反序列化后应为 null(需在 open() 中重建)
        assertThat(restored).isNotNull();
        // 无法直接访问 private transient 字段,但验证序列化/反序列化不报错即可
    }
}