package com.example.promgw.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * LabelsHasherTest 验证 labels hash 计算的一致性和确定性。
 */
class LabelsHasherTest {

    @Test
    void testSameLabelsSameHash() {
        Map<String, String> labels1 = new HashMap<>();
        labels1.put("job", "prometheus");
        labels1.put("instance", "localhost:9090");

        Map<String, String> labels2 = new HashMap<>();
        // 故意打乱顺序,hash 应一致(内部排序)
        labels2.put("instance", "localhost:9090");
        labels2.put("job", "prometheus");

        assertThat(LabelsHasher.hash(labels1)).isEqualTo(LabelsHasher.hash(labels2));
    }

    @Test
    void testDifferentLabelsDifferentHash() {
        Map<String, String> labels1 = new HashMap<>();
        labels1.put("job", "prometheus");

        Map<String, String> labels2 = new HashMap<>();
        labels2.put("job", "node-exporter");

        assertThat(LabelsHasher.hash(labels1)).isNotEqualTo(LabelsHasher.hash(labels2));
    }

    @Test
    void testEmptyLabels() {
        String hash = LabelsHasher.hash(new HashMap<>());
        assertThat(hash).isEqualTo("0000000000000000");
    }

    @Test
    void testNullLabels() {
        String hash = LabelsHasher.hash(null);
        assertThat(hash).isEqualTo("0000000000000000");
    }

    @Test
    void testHashLength() {
        Map<String, String> labels = new HashMap<>();
        labels.put("job", "x");
        String hash = LabelsHasher.hash(labels);
        assertThat(hash).hasSize(16);
    }

    @Test
    void testSeriesKeyConsistency() {
        Map<String, String> labels1 = new HashMap<>();
        labels1.put("job", "prom");
        labels1.put("env", "prod");

        Map<String, String> labels2 = new HashMap<>();
        labels2.put("env", "prod");
        labels2.put("job", "prom");

        // 同 labels 不同顺序,seriesKey 应一致
        String k1 = LabelsHasher.seriesKey("tenant1", "up", labels1);
        String k2 = LabelsHasher.seriesKey("tenant1", "up", labels2);
        assertThat(k1).isEqualTo(k2);
    }

    @Test
    void testSeriesKeyDifferentTenant() {
        Map<String, String> labels = new HashMap<>();
        labels.put("job", "prom");

        String k1 = LabelsHasher.seriesKey("tenant1", "up", labels);
        String k2 = LabelsHasher.seriesKey("tenant2", "up", labels);
        assertThat(k1).isNotEqualTo(k2);
    }
}
