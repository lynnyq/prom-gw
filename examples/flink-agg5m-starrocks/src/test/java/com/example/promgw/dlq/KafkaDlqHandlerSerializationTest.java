package com.example.promgw.dlq;

import static org.assertj.core.api.Assertions.assertThat;

import com.example.promgw.sink.DlqHandler;
import com.example.promgw.sink.StarRocksSink;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.flink.util.InstantiationUtil;
import org.junit.jupiter.api.Test;

/**
 * KafkaDlqHandlerSerializationTest 验证 DLQ 处理器及包含它的 Sink 可被 Flink 序列化。
 *
 * 复现生产报错场景:
 *   org.apache.flink.api.common.InvalidProgramException:
 *   The object ...KafkaDlqHandler is not serializable
 *
 * Flink ClosureCleaner 使用 {@link InstantiationUtil#serializeObject} 把 Sink 闭包
 * 序列化后分发到 TaskManager。本测试直接调用相同路径验证可序列化,并在反序列化后
 * 检查配置字段保留、transient 字段(mapper/producer)正确重建。
 */
class KafkaDlqHandlerSerializationTest {

    @Test
    void testDlqHandlerImplementsSerializable() {
        KafkaDlqHandler handler = new KafkaDlqHandler("broker1:9092,broker2:9092", "prom.local.dlq.sr.5m");
        assertThat(handler).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    void testStandaloneSerializeAndDeserialize() throws Exception {
        KafkaDlqHandler original = new KafkaDlqHandler(
                "kfcs-bdops-flow-kzx-0001:9092,kfcs-bdops-flow-kzx-0002:9092",
                "prom.local.dlq.sr.5m");

        // 用 Flink 同款序列化路径
        byte[] bytes = InstantiationUtil.serializeObject(original);

        KafkaDlqHandler restored = InstantiationUtil.deserializeObject(
                bytes, getClass().getClassLoader());

        // 配置字段保留
        assertThat(restored.getBootstrapServers()).isEqualTo(original.getBootstrapServers());
        assertThat(restored.getDlqTopic()).isEqualTo(original.getDlqTopic());
    }

    @Test
    void testStandaloneSerializeViaJavaIo() throws Exception {
        // 使用标准 Java 序列化验证
        KafkaDlqHandler original = new KafkaDlqHandler("broker:9092", "prom.dlq");

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(original);
        }

        try (ObjectInputStream ois = new ObjectInputStream(
                new ByteArrayInputStream(baos.toByteArray()))) {
            KafkaDlqHandler restored = (KafkaDlqHandler) ois.readObject();
            assertThat(restored.getBootstrapServers()).isEqualTo("broker:9092");
            assertThat(restored.getDlqTopic()).isEqualTo("prom.dlq");
        }
    }

    @Test
    void testTransientFieldsNullAfterDeserialization() throws Exception {
        // 反序列化后 transient 字段应为 null,open() 中重建
        KafkaDlqHandler original = new KafkaDlqHandler("broker:9092", "prom.dlq");
        byte[] bytes = InstantiationUtil.serializeObject(original);

        KafkaDlqHandler restored = InstantiationUtil.deserializeObject(
                bytes, getClass().getClassLoader());

        assertThat(restored.isMapperNull()).isTrue();
        assertThat(restored.isProducerNull()).isTrue();
    }

    @Test
    void testMapperRecreatedInOpen() throws Exception {
        KafkaDlqHandler original = new KafkaDlqHandler("broker:9092", "prom.dlq");
        byte[] bytes = InstantiationUtil.serializeObject(original);

        KafkaDlqHandler restored = InstantiationUtil.deserializeObject(
                bytes, getClass().getClassLoader());

        // 反序列化后 mapper 为 null
        assertThat(restored.isMapperNull()).isTrue();
        // open() 后 mapper 被重建(但不创建 producer,避免连真实 Kafka)
        // 这里只验证 mapper 重建逻辑,通过反射调用
        assertThat(restored.recreateMapperIfNeeded()).isTrue();
    }

    @Test
    void testStarRocksSinkWithDlqHandlerIsSerializable() throws Exception {
        // 复现生产报错:StarRocksSink 持有 DlqHandler 字段,Sink 闭包需可序列化
        DlqHandler dlqHandler = new KafkaDlqHandler("broker:9092", "prom.dlq");
        StarRocksSink sink = new StarRocksSink(
                "fe", 8030, "prom", "metrics_5m",
                "root", "", true, "local_5m", dlqHandler);

        // 这正是 Flink ClosureCleaner 调用的序列化路径
        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(bytes).isNotEmpty();
    }

    @Test
    void testStarRocksSinkWithoutDlqHandlerIsSerializable() throws Exception {
        // dlqHandler = null 场景也需可序列化
        StarRocksSink sink = new StarRocksSink(
                "fe", 8030, "prom", "metrics_5m",
                "root", "", true, "local_5m", null);

        byte[] bytes = InstantiationUtil.serializeObject(sink);
        assertThat(bytes).isNotEmpty();
    }
}
